//! Port of `EvaporativePermeationCalculator.java` and
//! `database/EvaporativePermeationCalculator.sql` — migration plan Phase 3,
//! Task 58.
//!
//! `EvaporativePermeationCalculator` produces the **evaporative permeation**
//! emission of total gaseous hydrocarbons — the THC (pollutant 1) that
//! diffuses through the polymer walls of a vehicle's fuel system. It
//! registers exactly one `(pollutant, process)` pair: THC × Evap Permeation
//! (process 11), and subscribes to that process at `MONTH` granularity.
//!
//! # What it computes
//!
//! For one emission, the calculator multiplies a temperature-corrected,
//! fuel-corrected, source-bin-weighted base rate by the source's operating
//! hours:
//!
//! ```text
//! emissionQuant = weightedTemperatureAdjust
//!               × fuelAdjustedEmissionRate
//!               × sourceHours
//! ```
//!
//! where `fuelAdjustedEmissionRate = meanBaseRate × weightedFuelAdjustment`,
//! `meanBaseRate` is the source-bin-activity-weighted emission rate, and
//! `weightedTemperatureAdjust` / `weightedFuelAdjustment` are the
//! operating-mode- and fuel-supply-weighted correction factors.
//!
//! # Algorithm
//!
//! [`EvaporativePermeationCalculator::calculate`] ports the SQL's
//! "Processing" section. The SQL builds seven MyISAM working tables across
//! six numbered steps; the port keeps each step as a function and threads
//! the working tables through as plain row vectors:
//!
//! | SQL step | SQL working table | This port |
//! |----------|-------------------|-----------|
//! | PC-1a | `SourceBinDistributionByAge` | folded into PC-1 |
//! | PC-1b | `SBWeightedPermeationRate` | `source_bin_weighted_permeation_rate` |
//! | PC-2a | `TemperatureAdjustByOpMode` | `temperature_adjust_by_op_mode` |
//! | PC-2b | `WeightedTemperatureAdjust` | `weighted_temperature_adjust` |
//! | PC-3 | `WeightedFuelAdjustment` | `weighted_fuel_adjustment` |
//! | PC-4 | `FuelAdjustedEmissionRate` | `fuel_adjusted_emission_rate` |
//! | PC-5 | `FuelAdjustedEmissionQuant` | `fuel_adjusted_emission_quant` |
//! | PC-6 | `MOVESWorkerOutput` | `assemble_emission_output` |
//!
//! Every join in the SQL is an `INNER JOIN`, so a row with no match on the
//! join key is dropped; the port reproduces that with map lookups that skip
//! on a miss. Three joins carry no `ON` clause and so are cartesian products
//! — `AverageTankTemperature × TemperatureAdjustment` in PC-2a and
//! `FuelSupply × County × HCPermeationCoeff` in PC-3; the port writes those
//! as nested loops.
//!
//! # The source-type loop
//!
//! The SQL wraps PC-1…PC-6 in `loop ##loop.sourceTypeID##; select
//! sourceTypeID from RunSpecSourceType;`, truncating and rebuilding every
//! working table once per source type. The loop only bounds MariaDB's
//! working-set size — each iteration's steps all filter `WHERE sourceTypeID
//! = ##loop.sourceTypeID##`, so the run's output is the union over the
//! source types in `RunSpecSourceType`. The port carries `sourceTypeID` in
//! every working-table key and processes all source types in one pass,
//! filtering to [`EvaporativePermeationInputs::run_spec_source_type`] where
//! the SQL's `WHERE` clauses do (PC-1a and PC-2b).
//!
//! # Regulatory-class sections
//!
//! PC-1b has two mutually exclusive forms. The `WithRegClassID` section
//! splits the base rate onto `regClassID` weighted by
//! `RegClassSourceTypeFraction.regClassFraction`; the `NoRegClassID` section
//! collapses `regClassID` to `0` and applies no fraction. MOVES's SQL
//! preprocessor keeps exactly one, selected by whether the run resolves
//! regulatory classes. [`RunContext::with_reg_class`] is that toggle.
//!
//! # Scope of this port
//!
//! [`calculate`](EvaporativePermeationCalculator::calculate) is the SQL
//! "Processing" section. The SQL's "Extract Data" section — the `cache
//! SELECT … INTO OUTFILE` statements that filter the default and execution
//! databases by run context — is data-plane wiring, not algorithm: an
//! [`EvaporativePermeationInputs`] *is* the post-extract tables, so the port
//! does not re-apply the extract `WHERE` clauses (`fuelRegionID`, `yearID`,
//! `polProcessID`, model-year-range filters). The Java `doExecute` wrapper
//! that generates the SQL is likewise wiring and is not ported.
//!
//! # Fidelity notes
//!
//! `EvaporativePermeationCalculator.sql` stores every working-table measure
//! in a `FLOAT` (32-bit) column while MariaDB evaluates the arithmetic in
//! `DOUBLE`. This port sums, multiplies and exponentiates in `f64` end to
//! end, so it does not reproduce the `f32` truncation MOVES applies between
//! steps — a sub-`1e-7` relative drift. Reproducing it bug-for-bug is the
//! calculator-integration-validation call (Task `mo-fvuf`, which this task
//! blocks), matching the Task 41 / Task 33 / Task 72 precedent. The `FLOAT`
//! input columns (`meanBaseRate`, `averageTankTemperature`,
//! `sourceBinActivityFraction`, `marketShare`, …) are model *inputs* —
//! already `f32`-quantised before [`calculate`](EvaporativePermeationCalculator::calculate)
//! sees them — and are modelled as `f64`. The Processing section opens with
//! `update FuelFormulation set ETOHVolume = 0 where ETOHVolume is null`;
//! [`FuelFormulationRow::etoh_volume`] is therefore an `Option<f64>` and the
//! port applies that null→0 coercion. There are no integer/integer literal
//! divisions in the SQL, so the MariaDB `div_precision_increment` rounding
//! gotcha does not arise.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm
//! is fully ported and unit-tested on
//! [`calculate`](EvaporativePermeationCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises an [`EvaporativePermeationInputs`]
//! and a [`RunContext`] from `ctx.tables()` / `ctx.position()`, calls
//! [`calculate`](EvaporativePermeationCalculator::calculate), and writes the
//! rows to the worker output.

use std::collections::{HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// Stable module name — matches the Java class and the
/// `EvaporativePermeationCalculator` entry in the calculator-chain DAG
/// (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "EvaporativePermeationCalculator";

/// Evap Permeation — `EmissionProcess` row 11. The calculator's only
/// process: it subscribes to it and registers one pollutant for it.
const EVAP_PERMEATION_PROCESS_ID: u16 = 11;

/// Total Gaseous Hydrocarbons — `Pollutant` row 1. The single pollutant the
/// calculator registers.
const TOTAL_HYDROCARBONS_POLLUTANT_ID: u16 = 1;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `EvaporativePermeation
// Calculator.sql`'s "Extract Data" section pulls. Following the Phase 3
// convention every `INT`/`SMALLINT` identifier is an `i32`, `sourceBinID`
// (`BIGINT`) is an `i64`, and every `FLOAT`/`DOUBLE` quantity is an `f64`.
// Only the columns the permeation algorithm reads are modelled.
// ===========================================================================

/// One `AgeCategory` row — the age-group bucket for a vehicle age.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageID` — vehicle age in years; the unique primary key.
    pub age_id: i32,
    /// `ageGroupID` — the age-group bucket the age falls in.
    pub age_group_id: i32,
}

/// One `AverageTankTemperature` row — a zone/month/hour-day/operating-mode
/// fuel-tank temperature.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageTankTemperatureRow {
    /// `tankTemperatureGroupID` — the tank-temperature group.
    pub tank_temperature_group_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `opModeID` — the operating mode the temperature applies to.
    pub op_mode_id: i32,
    /// `averageTankTemperature` — the mean tank temperature (°F).
    pub average_tank_temperature: f64,
}

/// One `County` row — supplies the `stateID` and the geographic-phase-in
/// area fraction for a county.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID` — the county primary key.
    pub county_id: i32,
    /// `stateID` — the state the county belongs to.
    pub state_id: i32,
    /// `GPAFract` — the fraction of the county inside a geographic phase-in
    /// area; weights the GPA fuel adjustment in PC-3.
    pub gpa_fract: f64,
}

/// One `EmissionRateByAge` row — a source bin's mean base rate for an
/// age group.
///
/// `EmissionRateByAge`'s primary key also carries `opModeID`; PC-1b joins
/// only `(sourceBinID, polProcessID, ageGroupID)`, so several operating-mode
/// rows can share a join key and all contribute to the sum. `opModeID` is
/// therefore not modelled — two rows that differ only in it are kept as two
/// rows here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `sourceBinID` — joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `ageGroupID` — the age-group bucket.
    pub age_group_id: i32,
    /// `meanBaseRate` — the mean emission base rate.
    pub mean_base_rate: f64,
}

/// One `ETOHBin` row — an ethanol-volume bin and its half-open bounds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EtohBinRow {
    /// `etohThreshID` — the bin identifier.
    pub etoh_thresh_id: i32,
    /// `etohThreshLow` — inclusive lower ethanol-volume bound. Schema-nullable;
    /// populated for the reference bins this calculator sees.
    pub etoh_thresh_low: f64,
    /// `etohThreshHigh` — exclusive upper ethanol-volume bound.
    pub etoh_thresh_high: f64,
}

/// One `FuelFormulation` row — a fuel blend's ethanol content.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID` — the formulation primary key.
    pub fuel_formulation_id: i32,
    /// `fuelSubtypeID` — joins to [`FuelSubtypeRow::fuel_subtype_id`].
    pub fuel_subtype_id: i32,
    /// `ETOHVolume` — ethanol volume percent. `FLOAT NULL`: the Processing
    /// section opens with `update FuelFormulation set ETOHVolume = 0 where
    /// ETOHVolume is null`, so [`EvaporativePermeationCalculator::calculate`]
    /// treats a `None` here as `0.0`.
    pub etoh_volume: Option<f64>,
}

/// One `FuelSubtype` row — resolves a fuel subtype into its fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubtypeRow {
    /// `fuelSubtypeID` — the subtype primary key.
    pub fuel_subtype_id: i32,
    /// `fuelTypeID` — the fuel type the subtype belongs to.
    pub fuel_type_id: i32,
}

/// One `FuelSupply` row — a fuel formulation's market share in the run's
/// fuel region.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
    /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
    /// `monthGroupID` — the month group.
    pub month_group_id: i32,
    /// `fuelFormulationID` — joins to [`FuelFormulationRow::fuel_formulation_id`].
    pub fuel_formulation_id: i32,
    /// `marketShare` — the formulation's share of the fuel supply.
    pub market_share: f64,
}

/// One `HCPermeationCoeff` row — the permeation fuel-adjustment coefficients.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HcPermeationCoeffRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `etohThreshID` — the ethanol bin the coefficients apply to.
    pub etoh_thresh_id: i32,
    /// `fuelMYGroupID` — the fuel model-year group.
    pub fuel_my_group_id: i32,
    /// `fuelAdjustment` — the base permeation fuel adjustment.
    pub fuel_adjustment: f64,
    /// `fuelAdjustmentGPA` — the geographic-phase-in fuel adjustment.
    pub fuel_adjustment_gpa: f64,
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
    /// `zoneID`. Schema-nullable; populated for the onroad links here.
    pub zone_id: i32,
    /// `roadTypeID` — road type.
    pub road_type_id: i32,
}

/// One `ModelYear` row — a model year in the run's window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelYearRow {
    /// `modelYearID` — the model year.
    pub model_year_id: i32,
}

/// One `OpModeDistribution` row — an operating mode's share of a
/// `(sourceType, link, hourDay)` activity for one `polProcessID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — the operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — the mode's share of activity.
    pub op_mode_fraction: f64,
}

/// One `PollutantProcessAssoc` row — resolves a `polProcessID` into its
/// `(pollutant, process)` pair.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
}

/// One `PollutantProcessMappedModelYear` row — maps a fuel model-year group
/// onto its model years. Used by PC-3.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessMappedModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — the model year.
    pub model_year_id: i32,
    /// `fuelMYGroupID` — the fuel model-year group.
    pub fuel_my_group_id: i32,
}

/// One `PollutantProcessModelYear` row — maps a model year onto its
/// model-year group. Used by PC-6.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — the model year.
    pub model_year_id: i32,
    /// `modelYearGroupID` — the model-year group.
    pub model_year_group_id: i32,
}

/// One `RegClassSourceTypeFraction` row — a regulatory class's share of a
/// `(fuelType, modelYear, sourceType)` group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegClassSourceTypeFractionRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID` — the regulatory class.
    pub reg_class_id: i32,
    /// `regClassFraction` — the class's share of the group's activity.
    pub reg_class_fraction: f64,
}

/// One `SourceBin` row — supplies the fuel type of a source bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID` — `BIGINT` primary key.
    pub source_bin_id: i64,
    /// `fuelTypeID` — the source bin's fuel type.
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

/// One `SourceHours` row — per `(hourDay, month, year, age, link,
/// sourceType)` source operating hours.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `sourceHours` — the source operating hours.
    pub source_hours: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID`
/// surrogate key into its `(sourceTypeID, modelYearID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
}

/// One `SourceTypeModelYearGroup` row — the tank-temperature group of a
/// `(sourceType, modelYearGroup)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearGroupRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
    /// `tankTemperatureGroupID` — the tank-temperature group.
    pub tank_temperature_group_id: i32,
}

/// One `TemperatureAdjustment` row — the permeation temperature-adjustment
/// coefficients for a fuel type and model-year range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TemperatureAdjustmentRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `minModelYearID` — inclusive lower model-year bound.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper model-year bound.
    pub max_model_year_id: i32,
    /// `tempAdjustTermA` — the multiplicative term.
    pub temp_adjust_term_a: f64,
    /// `tempAdjustTermB` — the exponential term.
    pub temp_adjust_term_b: f64,
}

/// One `Year` row — resolves a calendar year into its fuel year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
}

/// Inputs to [`EvaporativePermeationCalculator::calculate`] — the extracted
/// tables the SQL's "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the
/// per-run filtered execution database; until then it is the explicit
/// data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct EvaporativePermeationInputs {
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `AverageTankTemperature` rows.
    pub average_tank_temperature: Vec<AverageTankTemperatureRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `ETOHBin` rows.
    pub etoh_bin: Vec<EtohBinRow>,
    /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubtype` rows.
    pub fuel_subtype: Vec<FuelSubtypeRow>,
    /// `FuelSupply` rows.
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `HCPermeationCoeff` rows.
    pub hc_permeation_coeff: Vec<HcPermeationCoeffRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `Link` rows.
    pub link: Vec<LinkRow>,
    /// `ModelYear` rows.
    pub model_year: Vec<ModelYearRow>,
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `PollutantProcessMappedModelYear` rows.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `PollutantProcessModelYear` rows.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `RegClassSourceTypeFraction` rows. Consulted only when
    /// [`RunContext::with_reg_class`] is set.
    pub reg_class_source_type_fraction: Vec<RegClassSourceTypeFractionRow>,
    /// `RunSpecSourceType` — the source types the run processes. Drives the
    /// SQL's `loop ##loop.sourceTypeID##` (see the module docs).
    pub run_spec_source_type: Vec<i32>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceHours` rows.
    pub source_hours: Vec<SourceHoursRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `SourceTypeModelYearGroup` rows.
    pub source_type_model_year_group: Vec<SourceTypeModelYearGroupRow>,
    /// `TemperatureAdjustment` rows.
    pub temperature_adjustment: Vec<TemperatureAdjustmentRow>,
    /// `Year` rows.
    pub year: Vec<YearRow>,
}

/// The per-run scalar context [`EvaporativePermeationCalculator::calculate`]
/// reads — the `##context.*##` substitutions and the regulatory-class
/// section toggle the SQL preprocessor resolves before running the script.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunContext {
    /// `##context.year##` — the run's calendar year. Used to derive vehicle
    /// age (`year - modelYearID`) in PC-1a and stamped as `yearID` on the
    /// weighted permeation rate in PC-1b.
    pub year: i32,
    /// `##context.iterLocation.zoneRecordID##` — the run's zone. Stamped as
    /// `zoneID` on the weighted permeation rate in PC-1b.
    pub zone_id: i32,
    /// Whether the run resolves regulatory classes — selects PC-1b's
    /// `WithRegClassID` (`true`) or `NoRegClassID` (`false`) section.
    pub with_reg_class: bool,
}

/// One `MOVESWorkerOutput` row produced by the permeation calculation — the
/// PC-6 output.
///
/// `SCC` is written `NULL` by the SQL and is not an algorithm input; it is
/// left to the Task 50 output wiring and not modelled. `emissionQuant`
/// carries the computed emission.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PermeationEmissionRow {
    /// `stateID`.
    pub state_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `emissionQuant` — `weightedTemperatureAdjust × fuelAdjustedEmissionQuant`.
    pub emission_quant: f64,
}

impl PermeationEmissionRow {
    /// The integer dimension tuple — every column except `emissionQuant`.
    /// Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT`
    /// has no `ORDER BY`), so the port sorts purely to make the result
    /// reproducible.
    fn dimension_key(&self) -> [i32; 15] {
        [
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.road_type_id,
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.pollutant_id,
            self.process_id,
            self.source_type_id,
            self.reg_class_id,
            self.model_year_id,
            self.fuel_type_id,
        ]
    }
}

// ===========================================================================
// Working tables — private mirrors of the seven MyISAM tables the SQL's
// "Processing" section builds and drops. Each numbered PC step produces one;
// later steps consume it.
// ===========================================================================

/// PC-1a working table — `SourceBinDistribution` tagged with the age group
/// of its `(year - modelYear)` age.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SourceBinDistributionByAge {
    source_type_id: i32,
    model_year_id: i32,
    age_group_id: i32,
    pol_process_id: i32,
    source_bin_id: i64,
    source_bin_activity_fraction: f64,
}

/// PC-1b working table — `SBWeightedPermeationRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SbWeightedPermeationRate {
    zone_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    mean_base_rate: f64,
}

/// PC-2a working table — `TemperatureAdjustByOpMode`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TemperatureAdjustByOpMode {
    zone_id: i32,
    month_id: i32,
    hour_day_id: i32,
    tank_temperature_group_id: i32,
    op_mode_id: i32,
    pol_process_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    temperature_adjust_by_op_mode: f64,
}

/// PC-2b working table — `WeightedTemperatureAdjust`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedTemperatureAdjust {
    link_id: i32,
    month_id: i32,
    hour_day_id: i32,
    tank_temperature_group_id: i32,
    source_type_id: i32,
    pol_process_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    weighted_temperature_adjust: f64,
}

/// PC-3 working table — `WeightedFuelAdjustment`.
///
/// The SQL labels each row with the source-type loop variable, but no joined
/// table carries a source type, so the value is independent of it; the port
/// omits `sourceTypeID` (see [`weighted_fuel_adjustment`]).
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedFuelAdjustment {
    county_id: i32,
    fuel_year_id: i32,
    month_group_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    weighted_fuel_adjustment: f64,
}

/// PC-4 working table — `FuelAdjustedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelAdjustedEmissionRate {
    zone_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    fuel_adjusted_emission_rate: f64,
}

/// PC-5 working table — `FuelAdjustedEmissionQuant`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelAdjustedEmissionQuant {
    link_id: i32,
    hour_day_id: i32,
    month_id: i32,
    year_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    pol_process_id: i32,
    fuel_type_id: i32,
    fuel_adjusted_emission_quant: f64,
}

/// PC-1 — weight emission rates by source bin.
///
/// Ports the SQL's PC-1a (`SourceBinDistributionByAge`) and PC-1b
/// (`SBWeightedPermeationRate`) steps. PC-1a tags each `SourceBinDistribution`
/// row with the age group its `(calendar year − model year)` age falls in;
/// PC-1b joins the per-age-group emission rate and the source bin's fuel type
/// and sums `sourceBinActivityFraction × meanBaseRate` — weighted by
/// `regClassFraction` when the run uses regulatory classes — into one mean
/// base rate per output dimension.
fn source_bin_weighted_permeation_rate(
    inputs: &EvaporativePermeationInputs,
    ctx: &RunContext,
    source_types: &HashSet<i32>,
) -> Vec<SbWeightedPermeationRate> {
    // PC-1a — INNER JOIN SourceTypeModelYear USING (sourceTypeModelYearID),
    // INNER JOIN AgeCategory ON (ageID = year − modelYearID).
    let source_type_model_year: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|r| (r.source_type_model_year_id, r))
        .collect();
    // AgeCategory.ageID is the unique primary key — at most one age group
    // per age.
    let age_group_by_age: HashMap<i32, i32> = inputs
        .age_category
        .iter()
        .map(|r| (r.age_id, r.age_group_id))
        .collect();

    let mut by_age: Vec<SourceBinDistributionByAge> = Vec::new();
    for sbd in &inputs.source_bin_distribution {
        let Some(stmy) = source_type_model_year.get(&sbd.source_type_model_year_id) else {
            continue;
        };
        // PC-1a `WHERE sourceTypeID = ##loop.sourceTypeID##` — the loop ranges
        // over RunSpecSourceType (see the module docs on the source-type loop).
        if !source_types.contains(&stmy.source_type_id) {
            continue;
        }
        let Some(&age_group_id) = age_group_by_age.get(&(ctx.year - stmy.model_year_id)) else {
            continue;
        };
        by_age.push(SourceBinDistributionByAge {
            source_type_id: stmy.source_type_id,
            model_year_id: stmy.model_year_id,
            age_group_id,
            pol_process_id: sbd.pol_process_id,
            source_bin_id: sbd.source_bin_id,
            source_bin_activity_fraction: sbd.source_bin_activity_fraction,
        });
    }

    // PC-1b — INNER JOIN EmissionRateByAge ON (sourceBinID, polProcessID,
    // ageGroupID), INNER JOIN SourceBin USING (sourceBinID), and — in the
    // WithRegClassID section — INNER JOIN RegClassSourceTypeFraction.
    //
    // EmissionRateByAge carries one row per operating mode; the join does not
    // constrain opModeID, so every matching row contributes to the sum.
    let mut mean_base_rates: HashMap<(i64, i32, i32), Vec<f64>> = HashMap::new();
    for era in &inputs.emission_rate_by_age {
        mean_base_rates
            .entry((era.source_bin_id, era.pol_process_id, era.age_group_id))
            .or_default()
            .push(era.mean_base_rate);
    }
    let fuel_type_by_source_bin: HashMap<i64, i32> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb.fuel_type_id))
        .collect();
    let mut reg_class_fraction: HashMap<(i32, i32, i32), Vec<(i32, f64)>> = HashMap::new();
    for stf in &inputs.reg_class_source_type_fraction {
        reg_class_fraction
            .entry((stf.source_type_id, stf.fuel_type_id, stf.model_year_id))
            .or_default()
            .push((stf.reg_class_id, stf.reg_class_fraction));
    }

    // GROUP BY (polProcessID, sourceTypeID, regClassID, modelYearID, fuelTypeID).
    let mut weighted: HashMap<(i32, i32, i32, i32, i32), f64> = HashMap::new();
    for sbda in &by_age {
        let Some(rates) =
            mean_base_rates.get(&(sbda.source_bin_id, sbda.pol_process_id, sbda.age_group_id))
        else {
            continue;
        };
        let Some(&fuel_type_id) = fuel_type_by_source_bin.get(&sbda.source_bin_id) else {
            continue;
        };
        for &mean_base_rate in rates {
            let weighted_rate = sbda.source_bin_activity_fraction * mean_base_rate;
            if ctx.with_reg_class {
                let Some(fractions) = reg_class_fraction.get(&(
                    sbda.source_type_id,
                    fuel_type_id,
                    sbda.model_year_id,
                )) else {
                    continue;
                };
                for &(reg_class_id, reg_class_fraction) in fractions {
                    *weighted
                        .entry((
                            sbda.pol_process_id,
                            sbda.source_type_id,
                            reg_class_id,
                            sbda.model_year_id,
                            fuel_type_id,
                        ))
                        .or_default() += weighted_rate * reg_class_fraction;
                }
            } else {
                // NoRegClassID — regClassID collapses to 0, no fraction.
                *weighted
                    .entry((
                        sbda.pol_process_id,
                        sbda.source_type_id,
                        0,
                        sbda.model_year_id,
                        fuel_type_id,
                    ))
                    .or_default() += weighted_rate;
            }
        }
    }

    weighted
        .into_iter()
        .map(
            |(
                (pol_process_id, source_type_id, reg_class_id, model_year_id, fuel_type_id),
                mean_base_rate,
            )| SbWeightedPermeationRate {
                zone_id: ctx.zone_id,
                year_id: ctx.year,
                pol_process_id,
                source_type_id,
                reg_class_id,
                model_year_id,
                fuel_type_id,
                mean_base_rate,
            },
        )
        .collect()
}

/// PC-2a — temperature adjustment per operating mode.
///
/// `temperatureAdjustByOpMode = tempAdjustTermA · exp(tempAdjustTermB ·
/// averageTankTemperature)`. The SQL cross-joins `AverageTankTemperature`
/// with `TemperatureAdjustment` (the join carries no `ON` clause) and then
/// expands every pair across the model years in the adjustment's
/// `[minModelYearID, maxModelYearID]` range — the adjustment value itself
/// does not depend on the model year, which only adds an output dimension.
fn temperature_adjust_by_op_mode(
    inputs: &EvaporativePermeationInputs,
) -> Vec<TemperatureAdjustByOpMode> {
    let mut out = Vec::new();
    for att in &inputs.average_tank_temperature {
        for ta in &inputs.temperature_adjustment {
            let adjust = ta.temp_adjust_term_a
                * (ta.temp_adjust_term_b * att.average_tank_temperature).exp();
            for my in &inputs.model_year {
                if my.model_year_id < ta.min_model_year_id
                    || my.model_year_id > ta.max_model_year_id
                {
                    continue;
                }
                out.push(TemperatureAdjustByOpMode {
                    zone_id: att.zone_id,
                    month_id: att.month_id,
                    hour_day_id: att.hour_day_id,
                    tank_temperature_group_id: att.tank_temperature_group_id,
                    op_mode_id: att.op_mode_id,
                    pol_process_id: ta.pol_process_id,
                    fuel_type_id: ta.fuel_type_id,
                    model_year_id: my.model_year_id,
                    temperature_adjust_by_op_mode: adjust,
                });
            }
        }
    }
    out
}

/// PC-2b group-by key — the eight dimension columns of one
/// `WeightedTemperatureAdjust` row: `(linkID, monthID, hourDayID,
/// tankTemperatureGroupID, sourceTypeID, polProcessID, fuelTypeID,
/// modelYearID)`.
type WeightedTemperatureKey = (i32, i32, i32, i32, i32, i32, i32, i32);

/// PC-6 join key — the seven columns on which `FuelAdjustedEmissionQuant`
/// joins `WeightedTemperatureAdjust`: `(linkID, hourDayID, monthID,
/// sourceTypeID, polProcessID, fuelTypeID, modelYearID)`.
/// `tankTemperatureGroupID` is not a join column — it enters through the
/// `SourceTypeModelYearGroup` join instead.
type TemperatureJoinKey = (i32, i32, i32, i32, i32, i32, i32);

/// PC-2b — link-weighted temperature adjustment.
///
/// `weightedTemperatureAdjust = Σ(temperatureAdjustByOpMode · opModeFraction)`
/// over operating modes. PC-2a's rows are joined to `OpModeDistribution` on
/// `(hourDayID, polProcessID, opModeID)` and to `Link` on `(linkID, zoneID)`,
/// then summed per output dimension.
fn weighted_temperature_adjust(
    inputs: &EvaporativePermeationInputs,
    source_types: &HashSet<i32>,
    by_op_mode: &[TemperatureAdjustByOpMode],
) -> Vec<WeightedTemperatureAdjust> {
    let mut op_mode_distribution: HashMap<(i32, i32, i32), Vec<&OpModeDistributionRow>> =
        HashMap::new();
    for omd in &inputs.op_mode_distribution {
        op_mode_distribution
            .entry((omd.hour_day_id, omd.pol_process_id, omd.op_mode_id))
            .or_default()
            .push(omd);
    }
    let link: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();

    // GROUP BY (linkID, monthID, hourDayID, tankTemperatureGroupID,
    //           sourceTypeID, polProcessID, fuelTypeID, modelYearID).
    let mut weighted: HashMap<WeightedTemperatureKey, f64> = HashMap::new();
    for taom in by_op_mode {
        let Some(distributions) =
            op_mode_distribution.get(&(taom.hour_day_id, taom.pol_process_id, taom.op_mode_id))
        else {
            continue;
        };
        for omd in distributions {
            // PC-2b `WHERE sourceTypeID = ##loop.sourceTypeID##`.
            if !source_types.contains(&omd.source_type_id) {
                continue;
            }
            // INNER JOIN Link ON (linkID, l.zoneID = taom.zoneID).
            let Some(link_row) = link.get(&omd.link_id) else {
                continue;
            };
            if link_row.zone_id != taom.zone_id {
                continue;
            }
            *weighted
                .entry((
                    omd.link_id,
                    taom.month_id,
                    taom.hour_day_id,
                    taom.tank_temperature_group_id,
                    omd.source_type_id,
                    taom.pol_process_id,
                    taom.fuel_type_id,
                    taom.model_year_id,
                ))
                .or_default() += taom.temperature_adjust_by_op_mode * omd.op_mode_fraction;
        }
    }

    weighted
        .into_iter()
        .map(
            |(
                (
                    link_id,
                    month_id,
                    hour_day_id,
                    tank_temperature_group_id,
                    source_type_id,
                    pol_process_id,
                    fuel_type_id,
                    model_year_id,
                ),
                weighted_temperature_adjust,
            )| WeightedTemperatureAdjust {
                link_id,
                month_id,
                hour_day_id,
                tank_temperature_group_id,
                source_type_id,
                pol_process_id,
                fuel_type_id,
                model_year_id,
                weighted_temperature_adjust,
            },
        )
        .collect()
}

/// PC-3 — fuel-supply-weighted permeation adjustment.
///
/// `weightedFuelAdjustment = Σ(marketShare · (fuelAdjustment + GPAFract ·
/// (fuelAdjustmentGPA − fuelAdjustment)))` over the fuel formulations in the
/// fuel supply. The SQL cross-joins `FuelSupply`, `County` and
/// `HCPermeationCoeff`, then narrows the product through the mapped
/// model-year, calendar-year, fuel-formulation, ethanol-bin and fuel-subtype
/// joins.
///
/// The SQL labels each row with the source-type loop variable, but — as the
/// source comments note — no joined table carries a source type, so the
/// value is independent of it. The port omits `sourceTypeID` from the key;
/// PC-4 recovers it from `SBWeightedPermeationRate` (see
/// [`fuel_adjusted_emission_rate`]).
fn weighted_fuel_adjustment(
    inputs: &EvaporativePermeationInputs,
    ctx: &RunContext,
) -> Vec<WeightedFuelAdjustment> {
    // INNER JOIN PollutantProcessMappedModelYear ON (polProcessID, fuelMYGroupID).
    let mut mapped_model_year: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for ppmy in &inputs.pollutant_process_mapped_model_year {
        mapped_model_year
            .entry((ppmy.pol_process_id, ppmy.fuel_my_group_id))
            .or_default()
            .push(ppmy.model_year_id);
    }
    let fuel_formulation: HashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
    let fuel_type_by_subtype: HashMap<i32, i32> = inputs
        .fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst.fuel_type_id))
        .collect();

    // GROUP BY (countyID, fuelYearID, monthGroupID, polProcessID,
    //           modelYearID, fuelTypeID).
    let mut weighted: HashMap<(i32, i32, i32, i32, i32, i32), f64> = HashMap::new();
    // FuelSupply × County × HCPermeationCoeff — the three `ON`-less joins.
    for fs in &inputs.fuel_supply {
        for county in &inputs.county {
            for fa in &inputs.hc_permeation_coeff {
                let Some(model_years) =
                    mapped_model_year.get(&(fa.pol_process_id, fa.fuel_my_group_id))
                else {
                    continue;
                };
                // INNER JOIN Year ON (fuelYearID); WHERE y.yearID = year.
                // `Year.yearID` is the primary key, so this is a single-row
                // existence check.
                if !inputs
                    .year
                    .iter()
                    .any(|y| y.fuel_year_id == fs.fuel_year_id && y.year_id == ctx.year)
                {
                    continue;
                }
                // INNER JOIN FuelFormulation USING (fuelFormulationID).
                let Some(ff) = fuel_formulation.get(&fs.fuel_formulation_id) else {
                    continue;
                };
                // `update FuelFormulation set ETOHVolume = 0 where ETOHVolume
                // is null` — the Processing section's opening statement.
                let etoh_volume = ff.etoh_volume.unwrap_or(0.0);
                // INNER JOIN FuelSubtype USING (fuelSubtypeID).
                let Some(&fuel_type_id) = fuel_type_by_subtype.get(&ff.fuel_subtype_id) else {
                    continue;
                };
                let contribution = fs.market_share
                    * (fa.fuel_adjustment
                        + county.gpa_fract * (fa.fuel_adjustment_gpa - fa.fuel_adjustment));
                // INNER JOIN ETOHBin ON (etohThreshID,
                //   etohThreshLow <= ETOHVolume < etohThreshHigh).
                for ebin in &inputs.etoh_bin {
                    if ebin.etoh_thresh_id != fa.etoh_thresh_id
                        || etoh_volume < ebin.etoh_thresh_low
                        || etoh_volume >= ebin.etoh_thresh_high
                    {
                        continue;
                    }
                    for &model_year_id in model_years {
                        *weighted
                            .entry((
                                county.county_id,
                                fs.fuel_year_id,
                                fs.month_group_id,
                                fa.pol_process_id,
                                model_year_id,
                                fuel_type_id,
                            ))
                            .or_default() += contribution;
                    }
                }
            }
        }
    }

    weighted
        .into_iter()
        .map(
            |(
                (
                    county_id,
                    fuel_year_id,
                    month_group_id,
                    pol_process_id,
                    model_year_id,
                    fuel_type_id,
                ),
                weighted_fuel_adjustment,
            )| WeightedFuelAdjustment {
                county_id,
                fuel_year_id,
                month_group_id,
                pol_process_id,
                model_year_id,
                fuel_type_id,
                weighted_fuel_adjustment,
            },
        )
        .collect()
}

/// PC-4 — fuel-adjusted mean base rate.
///
/// `fuelAdjustedEmissionRate = meanBaseRate · weightedFuelAdjustment`,
/// joining PC-1b's `SBWeightedPermeationRate` to PC-3's
/// `WeightedFuelAdjustment` on `(polProcessID, modelYearID, fuelTypeID)` —
/// the SQL also matches `sourceTypeID`, which the port carries on the
/// permeation-rate side only (see [`weighted_fuel_adjustment`]) — and to
/// `Year` on `(yearID, fuelYearID)`.
fn fuel_adjusted_emission_rate(
    inputs: &EvaporativePermeationInputs,
    sb_weighted: &[SbWeightedPermeationRate],
    weighted_fuel: &[WeightedFuelAdjustment],
) -> Vec<FuelAdjustedEmissionRate> {
    let mut weighted_fuel_index: HashMap<(i32, i32, i32), Vec<&WeightedFuelAdjustment>> =
        HashMap::new();
    for wfa in weighted_fuel {
        weighted_fuel_index
            .entry((wfa.pol_process_id, wfa.model_year_id, wfa.fuel_type_id))
            .or_default()
            .push(wfa);
    }

    let mut out = Vec::new();
    for sbwpr in sb_weighted {
        let Some(matches) = weighted_fuel_index.get(&(
            sbwpr.pol_process_id,
            sbwpr.model_year_id,
            sbwpr.fuel_type_id,
        )) else {
            continue;
        };
        for wfa in matches {
            // INNER JOIN Year ON (y.yearID = sbwpr.yearID,
            //                     y.fuelYearID = wfa.fuelYearID).
            if !inputs
                .year
                .iter()
                .any(|y| y.year_id == sbwpr.year_id && y.fuel_year_id == wfa.fuel_year_id)
            {
                continue;
            }
            out.push(FuelAdjustedEmissionRate {
                zone_id: sbwpr.zone_id,
                year_id: sbwpr.year_id,
                pol_process_id: sbwpr.pol_process_id,
                source_type_id: sbwpr.source_type_id,
                reg_class_id: sbwpr.reg_class_id,
                model_year_id: sbwpr.model_year_id,
                fuel_type_id: sbwpr.fuel_type_id,
                fuel_adjusted_emission_rate: sbwpr.mean_base_rate * wfa.weighted_fuel_adjustment,
            });
        }
    }
    out
}

/// PC-5 — fuel-adjusted emission quantity.
///
/// `fuelAdjustedEmissionQuant = fuelAdjustedEmissionRate · sourceHours`,
/// joining `SourceHours` to PC-4's `FuelAdjustedEmissionRate` on
/// `(yearID, modelYearID = yearID − ageID, sourceTypeID)` and to `Link` on
/// `(linkID, zoneID)`.
fn fuel_adjusted_emission_quant(
    inputs: &EvaporativePermeationInputs,
    fuel_adjusted_rate: &[FuelAdjustedEmissionRate],
) -> Vec<FuelAdjustedEmissionQuant> {
    let mut rate_index: HashMap<(i32, i32, i32), Vec<&FuelAdjustedEmissionRate>> = HashMap::new();
    for faer in fuel_adjusted_rate {
        rate_index
            .entry((faer.year_id, faer.model_year_id, faer.source_type_id))
            .or_default()
            .push(faer);
    }
    let link: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();

    let mut out = Vec::new();
    for sh in &inputs.source_hours {
        // INNER JOIN FuelAdjustedEmissionRate ON (yearID,
        //   modelYearID = yearID − ageID, sourceTypeID).
        let model_year_id = sh.year_id - sh.age_id;
        let Some(matches) = rate_index.get(&(sh.year_id, model_year_id, sh.source_type_id)) else {
            continue;
        };
        // INNER JOIN Link ON (linkID, l.zoneID = fambr.zoneID).
        let Some(link_row) = link.get(&sh.link_id) else {
            continue;
        };
        for faer in matches {
            if link_row.zone_id != faer.zone_id {
                continue;
            }
            out.push(FuelAdjustedEmissionQuant {
                link_id: sh.link_id,
                hour_day_id: sh.hour_day_id,
                month_id: sh.month_id,
                year_id: faer.year_id,
                model_year_id: faer.model_year_id,
                source_type_id: faer.source_type_id,
                reg_class_id: faer.reg_class_id,
                pol_process_id: faer.pol_process_id,
                fuel_type_id: faer.fuel_type_id,
                fuel_adjusted_emission_quant: faer.fuel_adjusted_emission_rate * sh.source_hours,
            });
        }
    }
    out
}

/// PC-6 — assemble the emission output.
///
/// `emissionQuant = weightedTemperatureAdjust · fuelAdjustedEmissionQuant`.
/// PC-5's `FuelAdjustedEmissionQuant` is joined to PC-2b's
/// `WeightedTemperatureAdjust` on its seven-column key, to
/// `PollutantProcessAssoc` for the `(pollutant, process)` split, to
/// `PollutantProcessModelYear` for the model-year group, to
/// `SourceTypeModelYearGroup` (which gates the row on the source type /
/// model-year group's tank-temperature group matching the adjustment's), and
/// to `HourDay`, `Link` and `County` for the output time and geography.
fn assemble_emission_output(
    inputs: &EvaporativePermeationInputs,
    fuel_adjusted_quant: &[FuelAdjustedEmissionQuant],
    weighted_temp: &[WeightedTemperatureAdjust],
) -> Vec<PermeationEmissionRow> {
    let mut temp_index: HashMap<TemperatureJoinKey, Vec<&WeightedTemperatureAdjust>> =
        HashMap::new();
    for wta in weighted_temp {
        temp_index
            .entry((
                wta.link_id,
                wta.hour_day_id,
                wta.month_id,
                wta.source_type_id,
                wta.pol_process_id,
                wta.fuel_type_id,
                wta.model_year_id,
            ))
            .or_default()
            .push(wta);
    }
    let mut pollutant_process: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    for ppa in &inputs.pollutant_process_assoc {
        pollutant_process
            .entry(ppa.pol_process_id)
            .or_default()
            .push((ppa.pollutant_id, ppa.process_id));
    }
    let mut model_year_group: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        model_year_group
            .entry((ppmy.pol_process_id, ppmy.model_year_id))
            .or_default()
            .push(ppmy.model_year_group_id);
    }
    // SourceTypeModelYearGroup is unique on (sourceTypeID, modelYearGroupID).
    let tank_temperature_group: HashMap<(i32, i32), i32> = inputs
        .source_type_model_year_group
        .iter()
        .map(|r| {
            (
                (r.source_type_id, r.model_year_group_id),
                r.tank_temperature_group_id,
            )
        })
        .collect();
    let hour_day: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let link: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();
    let state_by_county: HashMap<i32, i32> = inputs
        .county
        .iter()
        .map(|c| (c.county_id, c.state_id))
        .collect();

    let mut out = Vec::new();
    for faeq in fuel_adjusted_quant {
        // INNER JOIN WeightedTemperatureAdjust on the seven-column key.
        let Some(temps) = temp_index.get(&(
            faeq.link_id,
            faeq.hour_day_id,
            faeq.month_id,
            faeq.source_type_id,
            faeq.pol_process_id,
            faeq.fuel_type_id,
            faeq.model_year_id,
        )) else {
            continue;
        };
        // INNER JOIN PollutantProcessAssoc USING (polProcessID).
        let Some(pol_procs) = pollutant_process.get(&faeq.pol_process_id) else {
            continue;
        };
        // INNER JOIN PollutantProcessModelYear ON (polProcessID, modelYearID).
        let Some(model_year_groups) =
            model_year_group.get(&(faeq.pol_process_id, faeq.model_year_id))
        else {
            continue;
        };
        // INNER JOIN HourDay / Link / County — all 1:1 on a primary key.
        let Some(hd) = hour_day.get(&faeq.hour_day_id) else {
            continue;
        };
        let Some(link_row) = link.get(&faeq.link_id) else {
            continue;
        };
        let Some(&state_id) = state_by_county.get(&link_row.county_id) else {
            continue;
        };
        for wta in temps {
            for &model_year_group_id in model_year_groups {
                // INNER JOIN SourceTypeModelYearGroup ON (sourceTypeID,
                //   modelYearGroupID, tankTemperatureGroupID): satisfied only
                //   when the group's tank-temperature group matches the
                //   adjustment's.
                let Some(&tank_temperature_group_id) =
                    tank_temperature_group.get(&(faeq.source_type_id, model_year_group_id))
                else {
                    continue;
                };
                if tank_temperature_group_id != wta.tank_temperature_group_id {
                    continue;
                }
                for &(pollutant_id, process_id) in pol_procs {
                    out.push(PermeationEmissionRow {
                        state_id,
                        county_id: link_row.county_id,
                        zone_id: link_row.zone_id,
                        link_id: faeq.link_id,
                        road_type_id: link_row.road_type_id,
                        year_id: faeq.year_id,
                        month_id: faeq.month_id,
                        day_id: hd.day_id,
                        hour_id: hd.hour_id,
                        pollutant_id,
                        process_id,
                        source_type_id: faeq.source_type_id,
                        reg_class_id: faeq.reg_class_id,
                        model_year_id: faeq.model_year_id,
                        fuel_type_id: faeq.fuel_type_id,
                        emission_quant: wta.weighted_temperature_adjust
                            * faeq.fuel_adjusted_emission_quant,
                    });
                }
            }
        }
    }
    out
}

/// The MOVES evaporative permeation calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input
/// flows through the [`EvaporativePermeationInputs`] / [`RunContext`]
/// arguments to [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct EvaporativePermeationCalculator {
    /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl EvaporativePermeationCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator with its master-loop subscription.
    ///
    /// The Java constructor signs up for the Evap Permeation process (11) at
    /// `MONTH` granularity with `EMISSION_CALCULATOR+1` priority; the
    /// `CalculatorInfo.txt` `Subscribe` directive and the calculator-chain
    /// DAG record the same single subscription.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR+1")
            .expect("\"EMISSION_CALCULATOR+1\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                ProcessId(EVAP_PERMEATION_PROCESS_ID),
                Granularity::Month,
                priority,
            )],
        }
    }

    /// Compute the permeation emission rows — the port of the
    /// `EvaporativePermeationCalculator.sql` "Processing" section.
    ///
    /// The six numbered SQL steps run in order: PC-1 weights emission rates
    /// by source bin, PC-2 builds the link-weighted temperature adjustment,
    /// PC-3 the fuel-supply-weighted adjustment, PC-4 the fuel-adjusted rate,
    /// PC-5 the fuel-adjusted quantity, and PC-6 assembles the output. The
    /// result is sorted by its integer dimension columns for deterministic
    /// output — MOVES leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(
        &self,
        inputs: &EvaporativePermeationInputs,
        ctx: &RunContext,
    ) -> Vec<PermeationEmissionRow> {
        // The SQL's `loop ##loop.sourceTypeID##` ranges over RunSpecSourceType;
        // the port carries sourceTypeID in the working-table keys and filters
        // to this set where the loop's `WHERE` clauses do.
        let source_types: HashSet<i32> = inputs.run_spec_source_type.iter().copied().collect();

        let sb_weighted = source_bin_weighted_permeation_rate(inputs, ctx, &source_types);
        let by_op_mode = temperature_adjust_by_op_mode(inputs);
        let weighted_temp = weighted_temperature_adjust(inputs, &source_types, &by_op_mode);
        let weighted_fuel = weighted_fuel_adjustment(inputs, ctx);
        let fuel_adjusted_rate = fuel_adjusted_emission_rate(inputs, &sb_weighted, &weighted_fuel);
        let fuel_adjusted_quant = fuel_adjusted_emission_quant(inputs, &fuel_adjusted_rate);
        let mut output = assemble_emission_output(inputs, &fuel_adjusted_quant, &weighted_temp);

        output.sort_unstable_by_key(PermeationEmissionRow::dimension_key);
        output
    }
}

impl Default for EvaporativePermeationCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// The one `(pollutant, process)` pair the calculator registers — Total
/// Gaseous Hydrocarbons × Evap Permeation.
///
/// Matches the single `Registration` directive recorded for
/// `EvaporativePermeationCalculator` in `CalculatorInfo.txt`
/// (`registrations_count: 1` in `calculator-dag.json`).
static REGISTRATIONS: [PollutantProcessAssociation; 1] = [PollutantProcessAssociation {
    pollutant_id: PollutantId(TOTAL_HYDROCARBONS_POLLUTANT_ID),
    process_id: ProcessId(EVAP_PERMEATION_PROCESS_ID),
}];

/// Default-DB / execution-DB tables the permeation computation consumes — the
/// data tables the SQL's "Extract Data" section pulls, including the
/// `RunSpecSourceType` table that drives the source-type loop.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "AverageTankTemperature",
    "County",
    "ETOHBin",
    "EmissionRateByAge",
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "HCPermeationCoeff",
    "HourDay",
    "Link",
    "ModelYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "PollutantProcessModelYear",
    "RegClassSourceTypeFraction",
    "RunSpecSourceType",
    "SourceBin",
    "SourceBinDistribution",
    "SourceHours",
    "SourceTypeModelYear",
    "SourceTypeModelYearGroup",
    "TemperatureAdjustment",
    "Year",
];

impl Calculator for EvaporativePermeationCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    /// The one `(pollutant, process)` pair: THC × Evap Permeation. See
    /// `REGISTRATIONS`.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): `calculator-dag.json`
    // records `depends_on: []`. `EvaporativePermeationCalculator` subscribes
    // directly to the master loop; the `Chain` directive makes it an upstream
    // of `HCSpeciationCalculator`, not the reverse.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Phase 2 skeleton — returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the input tables or accept
    /// the worker-output rows — its row storage lands with the Task 50
    /// `DataFrameStore`. The computation itself is ported and tested in
    /// [`EvaporativePermeationCalculator::calculate`]; see the
    /// [module documentation](self).
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(EvaporativePermeationCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run context the test fixtures use: calendar year 2020, zone 90,
    /// regulatory classes resolved.
    fn run_context() -> RunContext {
        RunContext {
            year: 2020,
            zone_id: 90,
            with_reg_class: true,
        }
    }

    /// A minimal one-of-everything input that threads exactly one row through
    /// all six PC steps.
    ///
    /// The hand-computed result: PC-1b `meanBaseRate = 1.0 × 2.0 × 1.0 = 2.0`;
    /// PC-2a `adjust = 1.0 × exp(0.0 × 70.0) = 1.0`; PC-2b
    /// `weightedTemperatureAdjust = 1.0 × 1.0 = 1.0`; PC-3
    /// `weightedFuelAdjustment = 1.0 × (3.0 + 0.0 × (9.0 − 3.0)) = 3.0`; PC-4
    /// `fuelAdjustedEmissionRate = 2.0 × 3.0 = 6.0`; PC-5
    /// `fuelAdjustedEmissionQuant = 6.0 × 10.0 = 60.0`; PC-6
    /// `emissionQuant = 1.0 × 60.0 = 60.0`. Every value is exactly
    /// representable in `f64`.
    fn minimal_inputs() -> EvaporativePermeationInputs {
        EvaporativePermeationInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 300,
            }],
            average_tank_temperature: vec![AverageTankTemperatureRow {
                tank_temperature_group_id: 11,
                zone_id: 90,
                month_id: 7,
                hour_day_id: 85,
                op_mode_id: 300,
                average_tank_temperature: 70.0,
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                state_id: 26,
                gpa_fract: 0.0,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 5000,
                pol_process_id: 111,
                age_group_id: 300,
                mean_base_rate: 2.0,
            }],
            etoh_bin: vec![EtohBinRow {
                etoh_thresh_id: 1,
                etoh_thresh_low: 0.0,
                etoh_thresh_high: 100.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                etoh_volume: Some(5.0),
            }],
            fuel_subtype: vec![FuelSubtypeRow {
                fuel_subtype_id: 10,
                fuel_type_id: 1,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 7,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            hc_permeation_coeff: vec![HcPermeationCoeffRow {
                pol_process_id: 111,
                etoh_thresh_id: 1,
                fuel_my_group_id: 50,
                fuel_adjustment: 3.0,
                fuel_adjustment_gpa: 9.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                county_id: 26_161,
                zone_id: 90,
                road_type_id: 5,
            }],
            model_year: vec![ModelYearRow {
                model_year_id: 2018,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 85,
                link_id: 5001,
                pol_process_id: 111,
                op_mode_id: 300,
                op_mode_fraction: 1.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 111,
                process_id: 11,
                pollutant_id: 1,
            }],
            pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
                pol_process_id: 111,
                model_year_id: 2018,
                fuel_my_group_id: 50,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 111,
                model_year_id: 2018,
                model_year_group_id: 400,
            }],
            reg_class_source_type_fraction: vec![RegClassSourceTypeFractionRow {
                fuel_type_id: 1,
                model_year_id: 2018,
                source_type_id: 21,
                reg_class_id: 30,
                reg_class_fraction: 1.0,
            }],
            run_spec_source_type: vec![21],
            source_bin: vec![SourceBinRow {
                source_bin_id: 5000,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 111,
                source_bin_id: 5000,
                source_bin_activity_fraction: 1.0,
            }],
            source_hours: vec![SourceHoursRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2,
                link_id: 5001,
                source_type_id: 21,
                source_hours: 10.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                model_year_id: 2018,
                source_type_id: 21,
            }],
            source_type_model_year_group: vec![SourceTypeModelYearGroupRow {
                source_type_id: 21,
                model_year_group_id: 400,
                tank_temperature_group_id: 11,
            }],
            temperature_adjustment: vec![TemperatureAdjustmentRow {
                pol_process_id: 111,
                fuel_type_id: 1,
                min_model_year_id: 1990,
                max_model_year_id: 2060,
                temp_adjust_term_a: 1.0,
                temp_adjust_term_b: 0.0,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
        }
    }

    /// Run the calculator over `inputs` with the standard [`run_context`].
    fn run(inputs: &EvaporativePermeationInputs) -> Vec<PermeationEmissionRow> {
        EvaporativePermeationCalculator::new().calculate(inputs, &run_context())
    }

    /// Assert two `emissionQuant`s match within `f64` slack.
    fn assert_quant(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "emissionQuant {actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = run(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.state_id, 26); // from County
        assert_eq!(r.county_id, 26_161); // from Link
        assert_eq!(r.zone_id, 90); // from Link
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.road_type_id, 5); // from Link
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5); // from HourDay
        assert_eq!(r.hour_id, 8); // from HourDay
        assert_eq!(r.pollutant_id, 1); // from PollutantProcessAssoc
        assert_eq!(r.process_id, 11); // from PollutantProcessAssoc
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30); // from RegClassSourceTypeFraction
        assert_eq!(r.model_year_id, 2018); // year - age
        assert_eq!(r.fuel_type_id, 1); // from SourceBin / FuelSubtype
        assert_quant(r.emission_quant, 60.0);
    }

    #[test]
    fn calculate_without_reg_class_collapses_reg_class_to_zero() {
        // NoRegClassID — RegClassSourceTypeFraction is not consulted, so the
        // base rate is not split: meanBaseRate stays 1.0 × 2.0 = 2.0 and the
        // final emissionQuant is unchanged, but regClassID is 0.
        let ctx = RunContext {
            with_reg_class: false,
            ..run_context()
        };
        let rows = EvaporativePermeationCalculator::new().calculate(&minimal_inputs(), &ctx);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reg_class_id, 0);
        assert_quant(rows[0].emission_quant, 60.0);
    }

    #[test]
    fn calculate_without_reg_class_ignores_missing_reg_class_fraction() {
        // With no RegClassSourceTypeFraction rows at all, the WithRegClassID
        // path drops the row but the NoRegClassID path still produces output.
        let mut inputs = minimal_inputs();
        inputs.reg_class_source_type_fraction.clear();
        assert!(run(&inputs).is_empty());

        let ctx = RunContext {
            with_reg_class: false,
            ..run_context()
        };
        let rows = EvaporativePermeationCalculator::new().calculate(&inputs, &ctx);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reg_class_id, 0);
    }

    #[test]
    fn calculate_applies_the_exponential_temperature_adjustment() {
        // tempAdjustTermB is non-zero: weightedTemperatureAdjust becomes
        // a × exp(b × T) = 2.0 × exp(0.01 × 70.0). emissionQuant is that
        // times the (unchanged) 60.0 from the fuel-adjusted quantity.
        let mut inputs = minimal_inputs();
        inputs.temperature_adjustment[0].temp_adjust_term_a = 2.0;
        inputs.temperature_adjustment[0].temp_adjust_term_b = 0.01;
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        let expected = 2.0 * (0.01_f64 * 70.0).exp() * 60.0;
        assert_quant(rows[0].emission_quant, expected);
    }

    #[test]
    fn calculate_weights_gpa_fuel_adjustment_by_county_fraction() {
        // GPAFract 0.25 blends the base and GPA fuel adjustments:
        // weightedFuelAdjustment = 1.0 × (3.0 + 0.25 × (9.0 − 3.0)) = 4.5.
        // emissionQuant = wta(1.0) × meanBaseRate(2.0) × 4.5 × sourceHours(10).
        let mut inputs = minimal_inputs();
        inputs.county[0].gpa_fract = 0.25;
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 1.0 * 2.0 * 4.5 * 10.0);
    }

    #[test]
    fn calculate_sums_source_bin_activity_across_bins() {
        // A second source bin on the same (regClass, fuelType) adds its
        // activity-weighted rate: meanBaseRate = 0.5×2.0 + 0.25×4.0 = 2.0.
        // The final emissionQuant is unchanged from the minimal 60.0.
        let mut inputs = minimal_inputs();
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.5;
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 5001,
            fuel_type_id: 1,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 111,
                source_bin_id: 5001,
                source_bin_activity_fraction: 0.25,
            });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 5001,
            pol_process_id: 111,
            age_group_id: 300,
            mean_base_rate: 4.0,
        });
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 60.0);
    }

    #[test]
    fn calculate_sums_emission_rate_across_operating_modes() {
        // EmissionRateByAge carries one row per operating mode; PC-1b's join
        // ignores opModeID, so a second rate row for the same source bin is
        // also summed: meanBaseRate = 1.0 × (2.0 + 1.5) = 3.5.
        let mut inputs = minimal_inputs();
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 5000,
            pol_process_id: 111,
            age_group_id: 300,
            mean_base_rate: 1.5,
        });
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 1.0 * 3.5 * 3.0 * 10.0);
    }

    #[test]
    fn calculate_skips_source_type_outside_run_spec() {
        // The only source type, 21, is not in RunSpecSourceType.
        let mut inputs = minimal_inputs();
        inputs.run_spec_source_type = vec![31];
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_source_bin_distribution_without_age_category() {
        // ageID = 2020 − 2018 = 2; with no AgeCategory row for age 2 the
        // PC-1a inner join drops the source-bin distribution.
        let mut inputs = minimal_inputs();
        inputs.age_category.clear();
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_distribution_without_emission_rate() {
        // No EmissionRateByAge row for the (sourceBin, polProcess, ageGroup).
        let mut inputs = minimal_inputs();
        inputs.emission_rate_by_age.clear();
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_distribution_without_source_bin() {
        // The SourceBin row supplying the fuel type is absent.
        let mut inputs = minimal_inputs();
        inputs.source_bin.clear();
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_temperature_adjust_outside_model_year_range() {
        // modelYearID 2018 falls outside [2025, 2060], so PC-2a produces no
        // TemperatureAdjustByOpMode row and PC-6 has nothing to join.
        let mut inputs = minimal_inputs();
        inputs.temperature_adjustment[0].min_model_year_id = 2025;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_op_mode_distribution_on_zone_mismatch() {
        // The link's zone no longer matches the tank temperature's zone, so
        // the PC-2b Link join fails.
        let mut inputs = minimal_inputs();
        inputs.link[0].zone_id = 91;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_fuel_adjustment_when_etoh_volume_out_of_bin() {
        // ETOHVolume 5.0 no longer falls in the [10, 100) bin.
        let mut inputs = minimal_inputs();
        inputs.etoh_bin[0].etoh_thresh_low = 10.0;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_treats_null_etoh_volume_as_zero() {
        // A null ETOHVolume is coerced to 0.0 (the SQL's opening UPDATE); 0.0
        // still falls in the [0, 100) bin, so the row survives.
        let mut inputs = minimal_inputs();
        inputs.fuel_formulation[0].etoh_volume = None;
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 60.0);

        // ...and with a bin that starts above 0, the null→0 volume is out of
        // range and the fuel adjustment drops.
        inputs.etoh_bin[0].etoh_thresh_low = 1.0;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_row_when_year_does_not_match_run_context() {
        // PC-3's `WHERE y.yearID = ##context.year##`: the Year row's calendar
        // year no longer matches the run context.
        let mut inputs = minimal_inputs();
        inputs.year[0].year_id = 2019;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_source_hours_without_matching_model_year() {
        // ageID 5 in year 2020 gives modelYearID 2015; the fuel-adjusted rate
        // is only built for 2018, so the PC-5 join finds nothing.
        let mut inputs = minimal_inputs();
        inputs.source_hours[0].age_id = 5;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_output_on_tank_temperature_group_mismatch() {
        // SourceTypeModelYearGroup maps (21, 400) to a tank-temperature group
        // that differs from the adjustment's, so the PC-6 join fails.
        let mut inputs = minimal_inputs();
        inputs.source_type_model_year_group[0].tank_temperature_group_id = 99;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_output_without_pollutant_process_model_year() {
        // No PollutantProcessModelYear row — the PC-6 model-year-group join
        // fails even though every other table matches.
        let mut inputs = minimal_inputs();
        inputs.pollutant_process_model_year.clear();
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two source types and two links produce several output rows; the
        // result must come back dimension-key sorted regardless of the
        // hash-map-driven computation order.
        let mut inputs = minimal_inputs();
        inputs.run_spec_source_type = vec![21, 31];
        inputs.source_type_model_year.push(SourceTypeModelYearRow {
            source_type_model_year_id: 312_018,
            model_year_id: 2018,
            source_type_id: 31,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 312_018,
                pol_process_id: 111,
                source_bin_id: 5000,
                source_bin_activity_fraction: 1.0,
            });
        inputs
            .reg_class_source_type_fraction
            .push(RegClassSourceTypeFractionRow {
                fuel_type_id: 1,
                model_year_id: 2018,
                source_type_id: 31,
                reg_class_id: 30,
                reg_class_fraction: 1.0,
            });
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            source_type_id: 31,
            hour_day_id: 85,
            link_id: 5001,
            pol_process_id: 111,
            op_mode_id: 300,
            op_mode_fraction: 1.0,
        });
        inputs.source_hours.push(SourceHoursRow {
            hour_day_id: 85,
            month_id: 7,
            year_id: 2020,
            age_id: 2,
            link_id: 5001,
            source_type_id: 31,
            source_hours: 10.0,
        });
        inputs
            .source_type_model_year_group
            .push(SourceTypeModelYearGroupRow {
                source_type_id: 31,
                model_year_group_id: 400,
                tank_temperature_group_id: 11,
            });

        let rows = run(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(run(&EvaporativePermeationInputs::default()).is_empty());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(
            EvaporativePermeationCalculator::new().name(),
            "EvaporativePermeationCalculator",
        );
        assert_eq!(
            EvaporativePermeationCalculator::NAME,
            "EvaporativePermeationCalculator",
        );
    }

    #[test]
    fn calculator_subscribes_to_evap_permeation_at_month() {
        let calc = EvaporativePermeationCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(11)); // Evap Permeation
        assert_eq!(subs[0].granularity, Granularity::Month);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR+1");
    }

    #[test]
    fn calculator_registers_thc_evap_permeation() {
        // One Registration directive: pollutant 1 (THC) × process 11.
        let calc = EvaporativePermeationCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 1);
        assert_eq!(regs[0].pollutant_id, PollutantId(1));
        assert_eq!(regs[0].process_id, ProcessId(11));
    }

    #[test]
    fn calculator_declares_input_tables_and_no_upstream() {
        let calc = EvaporativePermeationCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "AverageTankTemperature",
            "EmissionRateByAge",
            "HCPermeationCoeff",
            "RegClassSourceTypeFraction",
            "RunSpecSourceType",
            "TemperatureAdjustment",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
        // `calculator-dag.json` records `depends_on: []`.
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let calc = EvaporativePermeationCalculator::new();
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_ok());
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "EvaporativePermeationCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calc: Box<dyn Calculator> = Box::new(EvaporativePermeationCalculator::new());
        assert_eq!(calc.name(), "EvaporativePermeationCalculator");
        assert_eq!(calc.registrations().len(), 1);
    }
}
