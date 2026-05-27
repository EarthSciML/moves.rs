//! Port of `CriteriaRunningCalculator.java` and
//! `database/CriteriaRunningCalculator.sql` — migration plan Phase 3, Task 46.
//!
//! `CriteriaRunningCalculator` is the legacy scripted-SQL calculator for the
//! **criteria-pollutant running-exhaust** emission inventory: total gaseous
//! hydrocarbons (THC, pollutant 1), carbon monoxide (CO, pollutant 2) and
//! oxides of nitrogen (NOx, pollutant 3), all on the Running Exhaust process
//! (process 1). The Java constructor's capability set is the three
//! `polProcessID`s `101`/`201`/`301`.
//!
//! # Superseded by `BaseRateCalculator`
//!
//! This calculator is **not wired into the pinned MOVES runtime**.
//! `CalculatorInfo.txt` — the runtime registration file — has no
//! `Registration` directive for `CriteriaRunningCalculator`, and
//! `characterization/calculator-chains/calculator-dag.json` records
//! `registrations_count: 0` to match. `CriteriaRunningCalculator` is a
//! `GenericCalculatorBase` subclass with no Go worker: the base-rate approach
//! (migration-plan Task 45, `BaseRateCalculator`) superseded the older
//! per-pollutant scripted-SQL calculators like this one, and the THC/CO/NOx
//! running-exhaust `(pollutant, process)` pairs are registered to
//! `BaseRateCalculator` instead.
//!
//! The migration plan still lists the class as a task (Task 46), so this
//! module ports the **algorithm** — the SQL's `-- Section Processing` — for
//! reference and for cross-validation against `BaseRateCalculator`. To stay
//! consistent with the runtime, [`Calculator::registrations`] returns an
//! empty slice; the registry must not double-register the running-exhaust
//! pairs. This mirrors the sibling `CriteriaStartCalculator` (Task 47).
//!
//! # What it computes
//!
//! The criteria running-exhaust emission inventory is source-hours-operating
//! (SHO) activity multiplied by a fuel-, temperature-, air-conditioning-,
//! source-bin- and operating-mode-weighted emission rate, with an
//! inspection-and-maintenance (I/M) blend applied last:
//!
//! ```text
//! emissionQuant   = meanBaseRate   × SHO
//! emissionQuantIM = meanBaseRateIM × SHO
//! final           = max(emissionQuantIM × IMAdjustFract
//!                        + emissionQuant × (1 − IMAdjustFract), 0)
//! ```
//!
//! # Algorithm
//!
//! [`CriteriaRunningCalculator::calculate`] ports the SQL's "Processing"
//! section. The SQL builds working MyISAM tables across the numbered `CREC`
//! steps; the port keeps each step as a function and threads the working
//! tables through as plain row vectors:
//!
//! | SQL step | SQL working table | This port |
//! |----------|-------------------|-----------|
//! | CREC 1-a | `IMCoverageMergedUngrouped` | `im_coverage_merged` |
//! | CREC 2-a | `CountyFuelAdjustment` | `county_fuel_adjustment` |
//! | CREC 2-b | `FuelSupplyWithFuelType` | `fuel_supply_with_fuel_type` |
//! | CREC 2-b | `FuelSupplyAdjustment` | `fuel_supply_adjustment` |
//! | CREC 3 | `METAdjustment` | `met_adjustment` |
//! | CREC 4-a | `ACOnFraction` | `ac_on_fraction` |
//! | CREC 4-b | `ACActivityFraction` | `ac_activity_fraction` |
//! | CREC 4-c | `WeightedFullACAdjustment` | `weighted_full_ac_adjustment` |
//! | CREC 4-d | `ACAdjustment` | `ac_adjustment` |
//! | CREC-5 | `SBWeightedEmissionRate` | `sb_weighted_emission_rate` |
//! | CREC-6 | `FullyWeightedEmissionRate` | `fully_weighted_emission_rate` |
//! | CREC 7-a | `TempAndACAdjustment` | `temp_and_ac_adjustment` |
//! | CREC 7-b | `FuelAdjustedRate` | `fuel_adjusted_rate` |
//! | CREC 7-c | `WeightedAndAdjustedEmissionRate` | `weighted_and_adjusted_emission_rate` |
//! | CREC 8 | `WeightedAndAdjustedEmissionRate2` | `weighted_and_adjusted_emission_rate_2` |
//! | CREC 9 | `SHO2` | `build_sho2` |
//! | CREC 9 | `WeightedAndAdjustedEmissionRate3` | `weighted_and_adjusted_emission_rate_3` |
//! | CREC 9 | `MOVESWorkerOutput` | `assemble_emission_output` |
//!
//! Most joins in the SQL are `INNER JOIN`s; the port reproduces them with map
//! lookups that skip on a miss. CREC 7-b's `LEFT OUTER JOIN FuelSupplyAdjustment`
//! is the one outer join — a fully-weighted rate with no fuel-supply match
//! keeps a fuel adjustment of `1.0` (`ifnull(fuelAdjustment, 1.0)`). Several
//! steps cartesian-join (`INNER JOIN` with no `ON` clause): CREC 2-a joins
//! `criteriaRatio × County`, CREC 3 joins `ZoneMonthHour × TemperatureAdjustment
//! × ModelYear`, CREC 4-b joins `ACOnFraction × SourceTypeModelYear`, and
//! CREC 7-b joins `MonthOfAnyYear × FullyWeightedEmissionRate`; the port writes
//! those as nested loops.
//!
//! # Running temperature equation
//!
//! CREC 3 builds a multiplicative temperature adjustment per
//! `(polProcess, fuelType, modelYear)` against the `75 °F` reference: with
//! `d = temperature − 75`,
//!
//! ```text
//! temperatureAdjustment = 1 + d × (tempAdjustTermA + d × tempAdjustTermB)
//! ```
//!
//! Unlike the sibling start calculator's `LEAST(temperature, 75)` cap, the
//! running adjustment uses the raw signed delta — a start above `75 °F` and a
//! start below it both move the rate. There is one fixed quadratic equation;
//! there is no per-row `startTempEquationType` selector.
//!
//! # CREC 8 — the disabled humidity correction
//!
//! CREC 8 once applied a NOx humidity-correction factor. Bug 431 split the
//! step into a NOx branch (`polProcessID = 301`) and a non-NOx branch to
//! *disable* the humidity effect for non-NOx pollutants — but the pinned
//! 2013-11-19 script applies no humidity multiply in either branch.
//! `WeightedAndAdjustedEmissionRate2_TEMP1` (the `polProcessID = 301` rows,
//! inner-joined to `Link`, `ZoneMonthHour` and `FuelType`) and
//! `WeightedAndAdjustedEmissionRate2_TEMP2` (every other row) both select
//! `meanBaseRate`/`meanBaseRateIM` unchanged, and their `UNION` is
//! `WeightedAndAdjustedEmissionRate` row-for-row. So CREC 8 is a structural
//! pass-through. [`CriteriaRunningCalculator::calculate`] calls
//! `weighted_and_adjusted_emission_rate_2`, which reproduces the TEMP1 join
//! filter — under the extract's referential integrity it drops nothing — and
//! the `301` / non-`301` partition, then concatenates; the emission rate is
//! unchanged.
//!
//! # Scope of this port
//!
//! [`calculate`](CriteriaRunningCalculator::calculate) is the SQL "Processing"
//! section. The SQL's "Extract Data" section — the `cache SELECT … INTO
//! OUTFILE` statements that filter the default and execution databases by run
//! context — is data-plane wiring, not algorithm: a [`CriteriaRunningInputs`]
//! *is* the post-extract tables, so the port does not re-apply the extract
//! `WHERE` clauses (`fuelRegionID`, `yearID`, `monthID`, `zoneID`, `linkID`,
//! `countyID`, `polProcessID`, model-year-range filters). CREC 1-a's
//! `WHERE ppmy.polProcessID IN (##pollutantProcessIDs##)` is the only
//! processing-section pollutant filter, and it is redundant with the
//! `PollutantProcessMappedModelYear` extract, which already narrows to the
//! run's pollutant set — so the port carries no pollutant-id list in
//! [`RunContext`]. The SQL also extracts `Zone` and `PollutantProcessModelYear`;
//! neither feeds "Processing", so neither is modelled.
//!
//! Running Exhaust runs on every road type — `GenericCalculatorBase.doExecute`
//! gates only Start Exhaust and Extended Idle Exhaust to `roadTypeID = 1` — so,
//! unlike the start calculator, this port has no `processes_context` predicate;
//! the worker output's `roadTypeID` comes from the run's `Link` row.
//!
//! # Fidelity notes
//!
//! `CriteriaRunningCalculator.sql` stores every working-table measure in a
//! `FLOAT` (32-bit) column while MariaDB evaluates the arithmetic in `DOUBLE`.
//! This port sums and multiplies in `f64` end to end, so it does not reproduce
//! the `f32` truncation MOVES applies between steps — a sub-`1e-7` relative
//! drift. Reproducing it bug-for-bug is the calculator-integration-validation
//! call (Task `mo-fvuf`, which this task blocks), matching the Task 41 / Task
//! 33 / Task 47 precedent. The `FLOAT` input columns (`meanBaseRate`,
//! `meanBaseRateIM`, `marketShare`, `sourceBinActivityFraction`,
//! `opModeFraction`, `SHO`, `imFactor`, `complianceFactor`, `fullACAdjustment`,
//! `ACPenetrationFraction`, `functioningACFraction`, `ACActivityTermA`/`B`/`C`,
//! `tempAdjustTermA`/`B`) are model *inputs* — already `f32`-quantised before
//! [`calculate`](CriteriaRunningCalculator::calculate) sees them — and are
//! modelled as `f64`; `criteriaRatio.ratio`/`ratioGPA` and
//! `ZoneMonthHour.temperature`/`heatIndex` are `DOUBLE` in MOVES. There are no
//! integer/integer literal divisions in the SQL, so the MariaDB
//! `div_precision_increment` rounding gotcha does not arise.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm
//! is fully ported and unit-tested on
//! [`calculate`](CriteriaRunningCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises a [`CriteriaRunningInputs`] and a
//! [`RunContext`] from `ctx.tables()` / `ctx.position()`, calls
//! [`calculate`](CriteriaRunningCalculator::calculate), and writes the rows to
//! the worker output.

use std::collections::{HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the
/// `CriteriaRunningCalculator` entry in the calculator-chain DAG
/// (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CriteriaRunningCalculator";

/// Running Exhaust — `EmissionProcess` row 1. The calculator's only process:
/// it subscribes to it and the SQL produces Running Exhaust inventory.
const RUNNING_EXHAUST_PROCESS_ID: u16 = 1;

/// Reference temperature (°F) of the MOVES running-temperature equation. The
/// SQL forms the adjustment from the signed delta `temperature − 75`.
const RUNNING_TEMP_REFERENCE_F: f64 = 75.0;

/// NOx Running Exhaust `polProcessID` — `pollutantID 3 × 100 + processID 1`.
/// CREC 8 routes these rows through `WeightedAndAdjustedEmissionRate2_TEMP1`
/// and every other `polProcessID` through `_TEMP2`; see the [module docs](self).
const NOX_RUNNING_POL_PROCESS_ID: i32 = 301;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `CriteriaRunningCalculator
// .sql`'s "Extract Data" section pulls. Following the Phase 3 convention every
// `INT`/`SMALLINT` identifier is an `i32`, `sourceBinID` (`BIGINT`) is an
// `i64`, and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only the columns the
// "Processing" section reads are modelled.
// ===========================================================================

/// One `AgeCategory` row — the age-group bucket for a vehicle age.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageID` — vehicle age in years; the unique primary key.
    pub age_id: i32,
    /// `ageGroupID` — the age-group bucket the age falls in.
    pub age_group_id: i32,
}

/// One `County` row — only `GPAFract` feeds the algorithm (CREC 2-a blends the
/// geographic-phase-in and non-GPA fuel ratios by it).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID` — the county primary key. The extract filters `County` to
    /// the run's county, so this is informational.
    pub county_id: i32,
    /// `GPAFract` — geographic-phase-in-area fraction, in `[0, 1]`.
    pub gpa_fract: f64,
}

/// One `criteriaRatio` row — a fuel-formulation criteria-pollutant emission
/// ratio. The extract already applies `MYRMAP` to `modelYearID`, so
/// [`model_year_id`](Self::model_year_id) is the remapped value the CREC 2-a
/// join keys on. `ratioNoSulfur` is extracted but unused by the algorithm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CriteriaRatioRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `fuelFormulationID` — the fuel formulation the ratio applies to.
    pub fuel_formulation_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `modelYearID` — vehicle model year (post-`MYRMAP`).
    pub model_year_id: i32,
    /// `ratio` — the fuel adjustment ratio for a non-GPA area.
    pub ratio: f64,
    /// `ratioGPA` — the fuel adjustment ratio for a geographic-phase-in area.
    pub ratio_gpa: f64,
}

/// One `EmissionRateByAge` row — a base emission rate for one
/// `(polProcessID, sourceBinID, opModeID, ageGroupID)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `sourceBinID` — `BIGINT` source-bin key.
    pub source_bin_id: i64,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `ageGroupID` — the age-group bucket.
    pub age_group_id: i32,
    /// `meanBaseRate` — the base emission rate (non-I/M). `FLOAT` in MOVES.
    pub mean_base_rate: f64,
    /// `meanBaseRateIM` — the base emission rate for I/M-covered vehicles.
    pub mean_base_rate_im: f64,
}

/// One `FuelFormulation` row — only the `fuelSubtypeID` link is read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID` — the fuel-formulation primary key.
    pub fuel_formulation_id: i32,
    /// `fuelSubtypeID` — joins to [`FuelSubtypeRow::fuel_subtype_id`].
    pub fuel_subtype_id: i32,
}

/// One `FuelSubtype` row — resolves a fuel subtype into its fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubtypeRow {
    /// `fuelSubtypeID` — the fuel-subtype primary key.
    pub fuel_subtype_id: i32,
    /// `fuelTypeID` — the fuel type the subtype belongs to.
    pub fuel_type_id: i32,
}

/// One `FuelSupply` row — a fuel formulation's market share within a
/// `(fuelYear, monthGroup)` cell. The extract filters `FuelSupply` to the run's
/// fuel region, so `fuelRegionID` is not modelled.
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

/// One `FullACAdjustment` row — the full air-conditioning adjustment for a
/// `(sourceType, polProcess, opMode)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FullAcAdjustmentRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `fullACAdjustment` — the with-AC emission-rate multiplier.
    pub full_ac_adjustment: f64,
}

/// One `FuelType` row — only the `fuelTypeID` key feeds the algorithm; CREC 8
/// inner-joins it (and nothing else) for the disabled NOx-humidity branch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelTypeRow {
    /// `fuelTypeID` — the fuel-type primary key.
    pub fuel_type_id: i32,
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

/// One `IMCoverage` row — an inspection-and-maintenance program's compliance
/// over a model-year range. The extract filters `IMCoverage` to the run's
/// county/year and `useIMyn = 'Y'`, so those columns are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImCoverageRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `begModelYearID` — first model year covered by the program.
    pub beg_model_year_id: i32,
    /// `endModelYearID` — last model year covered by the program.
    pub end_model_year_id: i32,
    /// `inspectFreq` — inspection frequency.
    pub inspect_freq: i32,
    /// `testStandardsID` — test-standards identifier.
    pub test_standards_id: i32,
    /// `complianceFactor` — program compliance, as a percentage; the SQL
    /// scales it by `0.01`.
    pub compliance_factor: f64,
}

/// One `IMFactor` row — an inspection-and-maintenance benefit factor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImFactorRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `inspectFreq` — inspection frequency.
    pub inspect_freq: i32,
    /// `testStandardsID` — test-standards identifier.
    pub test_standards_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `IMModelYearGroupID` — the I/M model-year group.
    pub im_model_year_group_id: i32,
    /// `ageGroupID` — the age-group bucket.
    pub age_group_id: i32,
    /// `IMFactor` — the I/M benefit factor.
    pub im_factor: f64,
}

/// One `Link` row — the run's road link. The extract filters `Link` to the
/// run's link, so it is a single row; it supplies the `(zoneID, roadTypeID)`
/// the worker output stamps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID` — the link primary key.
    pub link_id: i32,
    /// `zoneID` — the zone the link belongs to.
    pub zone_id: i32,
    /// `roadTypeID` — the link's road type; stamped on the worker output.
    pub road_type_id: i32,
}

/// One `ModelYear` row — the bare list of modelled vehicle model years; CREC 3
/// cross-joins it and keeps the years inside each `TemperatureAdjustment`
/// row's `[minModelYearID, maxModelYearID]` band.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelYearRow {
    /// `modelYearID` — a vehicle model year.
    pub model_year_id: i32,
}

/// One `MonthGroupHour` row — the air-conditioning activity-term coefficients
/// for a `(monthGroup, hour)` cell. The `*CV` coefficient-of-variation columns
/// are not read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthGroupHourRow {
    /// `monthGroupID` — the month group.
    pub month_group_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `ACActivityTermA` — AC-activity equation coefficient A.
    pub ac_activity_term_a: f64,
    /// `ACActivityTermB` — AC-activity equation coefficient B.
    pub ac_activity_term_b: f64,
    /// `ACActivityTermC` — AC-activity equation coefficient C.
    pub ac_activity_term_c: f64,
}

/// One `MonthOfAnyYear` row — resolves a month into its month group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthOfAnyYearRow {
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `monthGroupID` — the month group the month belongs to.
    pub month_group_id: i32,
}

/// One `OpModeDistribution` row — the share of activity in one operating mode.
/// The extract filters `OpModeDistribution` to the run's link, so `linkID` is
/// not modelled — the port stamps [`RunContext::link_id`] where the SQL carries
/// the column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — the operating mode's share of activity.
    pub op_mode_fraction: f64,
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

/// One `PollutantProcessMappedModelYear` row — a mapped vehicle model year
/// with its I/M model-year group. CREC 3 keys the temperature adjustment off
/// the `ModelYear` table instead, so `modelYearGroupID` is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessMappedModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `IMModelYearGroupID` — the I/M model-year group the year belongs to.
    pub im_model_year_group_id: i32,
}

/// One `SHO` row — source-hours-operating activity for a `(hourDay, month,
/// year, age, sourceType)` cell. The extract filters `SHO` to the run's link,
/// month and year, so `linkID` is not modelled.
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
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `SHO` — source-hours operating. `FLOAT` in MOVES.
    pub sho: f64,
}

/// One `SourceBin` row — only `fuelTypeID` is read.
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

/// One `SourceTypeAge` row — the functioning-AC fraction for a
/// `(sourceType, age)` cell. The `survivalRate`/`relativeMAR`/`*CV` columns
/// are not read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `ageID` — vehicle age in years.
    pub age_id: i32,
    /// `functioningACFraction` — fraction of vehicles with a working AC unit.
    pub functioning_ac_fraction: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID` surrogate
/// key into its `(sourceTypeID, modelYearID)` components and carries the
/// AC-penetration fraction. The `ACPenetrationFractionCV` column is not read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `ACPenetrationFraction` — fraction of vehicles equipped with AC.
    pub ac_penetration_fraction: f64,
}

/// One `TemperatureAdjustment` row — the running-temperature equation
/// coefficients for a `(polProcess, fuelType)` cell over a model-year band.
/// The `regClassID` and `tempAdjustTermC` columns are not read by CREC 3.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TemperatureAdjustmentRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `minModelYearID` — first model year the coefficients apply to.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — last model year the coefficients apply to.
    pub max_model_year_id: i32,
    /// `tempAdjustTermA` — temperature-equation coefficient A.
    pub temp_adjust_term_a: f64,
    /// `tempAdjustTermB` — temperature-equation coefficient B.
    pub temp_adjust_term_b: f64,
}

/// One `Year` row — resolves a calendar year into its fuel year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `fuelYearID` — the fuel year the calendar year maps to.
    pub fuel_year_id: i32,
}

/// One `ZoneMonthHour` row — the temperature and heat index for a
/// `(zone, month, hour)` cell. The `relHumidity`/`specificHumidity`/
/// `molWaterFraction` columns are not read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `zoneID` — the zone.
    pub zone_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `temperature` — ambient temperature, °F. `DOUBLE` in MOVES.
    pub temperature: f64,
    /// `heatIndex` — apparent temperature driving AC use. `DOUBLE` in MOVES.
    pub heat_index: f64,
}

/// Inputs to [`CriteriaRunningCalculator::calculate`] — the tables the SQL's
/// "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct CriteriaRunningInputs {
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// `criteriaRatio` rows.
    pub criteria_ratio: Vec<CriteriaRatioRow>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubtype` rows.
    pub fuel_subtype: Vec<FuelSubtypeRow>,
    /// `FuelSupply` rows.
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FullACAdjustment` rows.
    pub full_ac_adjustment: Vec<FullAcAdjustmentRow>,
    /// `FuelType` rows.
    pub fuel_type: Vec<FuelTypeRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `IMCoverage` rows.
    pub im_coverage: Vec<ImCoverageRow>,
    /// `IMFactor` rows.
    pub im_factor: Vec<ImFactorRow>,
    /// `Link` rows.
    pub link: Vec<LinkRow>,
    /// `ModelYear` rows.
    pub model_year: Vec<ModelYearRow>,
    /// `MonthGroupHour` rows.
    pub month_group_hour: Vec<MonthGroupHourRow>,
    /// `MonthOfAnyYear` rows.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `PollutantProcessMappedModelYear` rows.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `SHO` rows.
    pub sho: Vec<ShoRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceTypeAge` rows.
    pub source_type_age: Vec<SourceTypeAgeRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `TemperatureAdjustment` rows.
    pub temperature_adjustment: Vec<TemperatureAdjustmentRow>,
    /// `Year` rows.
    pub year: Vec<YearRow>,
    /// `ZoneMonthHour` rows.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
}

/// The per-run scalar context [`CriteriaRunningCalculator::calculate`] reads —
/// the `##context.*##` substitutions the SQL preprocessor resolves before
/// running the script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunContext {
    /// `##context.year##` — the run's calendar year. Used to derive vehicle
    /// model year (`year - ageID`) in CREC 4-b and CREC-5, and stamped as
    /// `yearID` on the CREC-5 emission rates.
    pub year: i32,
    /// `##context.iterLocation.countyRecordID##` — the run's county. Stamped
    /// on `FuelSupplyWithFuelType` (CREC 2-b) and on the worker output.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##` — the run's zone. Stamped as
    /// `zoneID` on the CREC-5 source-bin-weighted emission rates.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##` — the run's link. The SQL
    /// carries `linkID` from the extract-filtered `OpModeDistribution`; the
    /// port stamps this value where the SQL reads the column.
    pub link_id: i32,
    /// `##context.iterLocation.stateRecordID##` — the run's state. Stamped as
    /// `stateID` on the worker output.
    pub state_id: i32,
}

/// One `MOVESWorkerOutput` row produced by the criteria running calculation —
/// the CREC-9 output, with the I/M blend applied.
///
/// The SQL writes an `SCC` column `NULL`; it is not an algorithm input and is
/// left to the Task 50 output wiring. `emission_quant` carries the final,
/// I/M-adjusted emission; the SQL's intermediate `emissionQuantIM` column is
/// dropped before the worker output is returned, so it is not modelled here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CriteriaRunningEmissionRow {
    /// `stateID`.
    pub state_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `roadTypeID` — from the run's `Link` row; running exhaust occurs on
    /// every road type.
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
    /// `processID` — always `1` (Running Exhaust).
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `emissionQuant` — the final, I/M-adjusted emission for this cell.
    pub emission_quant: f64,
}

impl CriteriaRunningEmissionRow {
    /// The integer dimension tuple — every column except `emission_quant`.
    /// Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT` has
    /// no `ORDER BY`), so the port sorts purely to make the result
    /// reproducible.
    fn dimension_key(&self) -> [i32; 14] {
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
            self.model_year_id,
            self.fuel_type_id,
        ]
    }
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction { table: table.into(), row, column: column.into(), message: msg }
}

impl TableRow for AgeCategoryRow {
    fn table_name() -> &'static str { "AgeCategory" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("ageID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
            Series::new("ageGroupID".into(), rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AgeCategory";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let age_id = get_i32("ageID")?;
        let age_group_id = get_i32("ageGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(AgeCategoryRow {
                age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
            })
        }).collect()
    }
}

impl TableRow for CountyRow {
    fn table_name() -> &'static str { "County" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("GPAFract".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
            Series::new("GPAFract".into(), rows.iter().map(|r| r.gpa_fract).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let county_id = get_i32("countyID")?;
        let gpa_fract = get_f64("GPAFract")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(CountyRow {
                county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                gpa_fract: gpa_fract.get(i).ok_or_else(|| null("GPAFract"))?,
            })
        }).collect()
    }
}

impl TableRow for CriteriaRatioRow {
    fn table_name() -> &'static str { "criteriaRatio" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("ratio".into(), DataType::Float64),
            ("ratioGPA".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("ratio".into(), rows.iter().map(|r| r.ratio).collect::<Vec<f64>>()).into(),
            Series::new("ratioGPA".into(), rows.iter().map(|r| r.ratio_gpa).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "criteriaRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let ratio = get_f64("ratio")?;
        let ratio_gpa = get_f64("ratioGPA")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(CriteriaRatioRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                ratio: ratio.get(i).ok_or_else(|| null("ratio"))?,
                ratio_gpa: ratio_gpa.get(i).ok_or_else(|| null("ratioGPA"))?,
            })
        }).collect()
    }
}

impl TableRow for EmissionRateByAgeRow {
    fn table_name() -> &'static str { "EmissionRateByAge" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceBinID".into(), DataType::Int64),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("opModeID".into(), rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>()).into(),
            Series::new("ageGroupID".into(), rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>()).into(),
            Series::new("meanBaseRate".into(), rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>()).into(),
            Series::new("meanBaseRateIM".into(), rows.iter().map(|r| r.mean_base_rate_im).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionRateByAge";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let mean_base_rate = get_f64("meanBaseRate")?;
        let mean_base_rate_im = get_f64("meanBaseRateIM")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(EmissionRateByAgeRow {
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
                mean_base_rate_im: mean_base_rate_im.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str { "FuelFormulation" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelSubtypeID".into(), rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let fuel_subtype_id = get_i32("fuelSubtypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelFormulationRow {
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                fuel_subtype_id: fuel_subtype_id.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelSubtypeRow {
    fn table_name() -> &'static str { "FuelSubtype" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelSubtypeID".into(), rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSubtype";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_subtype_id = get_i32("fuelSubtypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSubtypeRow {
                fuel_subtype_id: fuel_subtype_id.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str { "FuelSupply" }
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
        DataFrame::new(n, vec![
            Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
            Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
            Series::new("marketShare".into(), rows.iter().map(|r| r.market_share).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_year_id = get_i32("fuelYearID")?;
        let month_group_id = get_i32("monthGroupID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSupplyRow {
                fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
            })
        }).collect()
    }
}

impl TableRow for FullAcAdjustmentRow {
    fn table_name() -> &'static str { "FullACAdjustment" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("fullACAdjustment".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("opModeID".into(), rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>()).into(),
            Series::new("fullACAdjustment".into(), rows.iter().map(|r| r.full_ac_adjustment).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FullACAdjustment";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let full_ac_adjustment = get_f64("fullACAdjustment")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FullAcAdjustmentRow {
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                full_ac_adjustment: full_ac_adjustment.get(i).ok_or_else(|| null("fullACAdjustment"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelTypeRow {
    fn table_name() -> &'static str { "FuelType" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("fuelTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelTypeRow {
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for HourDayRow {
    fn table_name() -> &'static str { "HourDay" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "HourDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day_id = get_i32("hourDayID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(HourDayRow {
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
            })
        }).collect()
    }
}

impl TableRow for ImCoverageRow {
    fn table_name() -> &'static str { "IMCoverage" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("begModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("complianceFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("begModelYearID".into(), rows.iter().map(|r| r.beg_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("endModelYearID".into(), rows.iter().map(|r| r.end_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("inspectFreq".into(), rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>()).into(),
            Series::new("testStandardsID".into(), rows.iter().map(|r| r.test_standards_id).collect::<Vec<i32>>()).into(),
            Series::new("complianceFactor".into(), rows.iter().map(|r| r.compliance_factor).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMCoverage";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let beg_model_year_id = get_i32("begModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let compliance_factor = get_f64("complianceFactor")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ImCoverageRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                beg_model_year_id: beg_model_year_id.get(i).ok_or_else(|| null("begModelYearID"))?,
                end_model_year_id: end_model_year_id.get(i).ok_or_else(|| null("endModelYearID"))?,
                inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                test_standards_id: test_standards_id.get(i).ok_or_else(|| null("testStandardsID"))?,
                compliance_factor: compliance_factor.get(i).ok_or_else(|| null("complianceFactor"))?,
            })
        }).collect()
    }
}

impl TableRow for ImFactorRow {
    fn table_name() -> &'static str { "IMFactor" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("IMModelYearGroupID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("IMFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("inspectFreq".into(), rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>()).into(),
            Series::new("testStandardsID".into(), rows.iter().map(|r| r.test_standards_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("IMModelYearGroupID".into(), rows.iter().map(|r| r.im_model_year_group_id).collect::<Vec<i32>>()).into(),
            Series::new("ageGroupID".into(), rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>()).into(),
            Series::new("IMFactor".into(), rows.iter().map(|r| r.im_factor).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMFactor";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let im_factor = get_f64("IMFactor")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ImFactorRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                test_standards_id: test_standards_id.get(i).ok_or_else(|| null("testStandardsID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                im_model_year_group_id: im_model_year_group_id.get(i).ok_or_else(|| null("IMModelYearGroupID"))?,
                age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                im_factor: im_factor.get(i).ok_or_else(|| null("IMFactor"))?,
            })
        }).collect()
    }
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str { "Link" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let link_id = get_i32("linkID")?;
        let zone_id = get_i32("zoneID")?;
        let road_type_id = get_i32("roadTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(LinkRow {
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for ModelYearRow {
    fn table_name() -> &'static str { "ModelYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("modelYearID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let model_year_id = get_i32("modelYearID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ModelYearRow {
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
            })
        }).collect()
    }
}

impl TableRow for MonthGroupHourRow {
    fn table_name() -> &'static str { "MonthGroupHour" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthGroupID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("ACActivityTermA".into(), DataType::Float64),
            ("ACActivityTermB".into(), DataType::Float64),
            ("ACActivityTermC".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
            Series::new("ACActivityTermA".into(), rows.iter().map(|r| r.ac_activity_term_a).collect::<Vec<f64>>()).into(),
            Series::new("ACActivityTermB".into(), rows.iter().map(|r| r.ac_activity_term_b).collect::<Vec<f64>>()).into(),
            Series::new("ACActivityTermC".into(), rows.iter().map(|r| r.ac_activity_term_c).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthGroupHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let month_group_id = get_i32("monthGroupID")?;
        let hour_id = get_i32("hourID")?;
        let ac_activity_term_a = get_f64("ACActivityTermA")?;
        let ac_activity_term_b = get_f64("ACActivityTermB")?;
        let ac_activity_term_c = get_f64("ACActivityTermC")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(MonthGroupHourRow {
                month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                ac_activity_term_a: ac_activity_term_a.get(i).ok_or_else(|| null("ACActivityTermA"))?,
                ac_activity_term_b: ac_activity_term_b.get(i).ok_or_else(|| null("ACActivityTermB"))?,
                ac_activity_term_c: ac_activity_term_c.get(i).ok_or_else(|| null("ACActivityTermC"))?,
            })
        }).collect()
    }
}

impl TableRow for MonthOfAnyYearRow {
    fn table_name() -> &'static str { "MonthOfAnyYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let month_id = get_i32("monthID")?;
        let month_group_id = get_i32("monthGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(MonthOfAnyYearRow {
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
            })
        }).collect()
    }
}

impl TableRow for OpModeDistributionRow {
    fn table_name() -> &'static str { "OpModeDistribution" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("opModeID".into(), rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>()).into(),
            Series::new("opModeFraction".into(), rows.iter().map(|r| r.op_mode_fraction).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(OpModeDistributionRow {
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                op_mode_fraction: op_mode_fraction.get(i).ok_or_else(|| null("opModeFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for PollutantProcessAssocRow {
    fn table_name() -> &'static str { "PollutantProcessAssoc" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
            Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessAssocRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
            })
        }).collect()
    }
}

impl TableRow for PollutantProcessMappedModelYearRow {
    fn table_name() -> &'static str { "PollutantProcessMappedModelYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("IMModelYearGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("IMModelYearGroupID".into(), rows.iter().map(|r| r.im_model_year_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessMappedModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_id = get_i32("modelYearID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessMappedModelYearRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                im_model_year_group_id: im_model_year_group_id.get(i).ok_or_else(|| null("IMModelYearGroupID"))?,
            })
        }).collect()
    }
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str { "SHO" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
            Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("SHO".into(), rows.iter().map(|r| r.sho).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHO";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let sho = get_f64("SHO")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ShoRow {
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                sho: sho.get(i).ok_or_else(|| null("SHO"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceBinRow {
    fn table_name() -> &'static str { "SourceBin" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceBinID".into(), DataType::Int64),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBin";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinRow {
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceBinDistributionRow {
    fn table_name() -> &'static str { "SourceBinDistribution" }
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
        DataFrame::new(n, vec![
            Series::new("sourceTypeModelYearID".into(), rows.iter().map(|r| r.source_type_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("sourceBinActivityFraction".into(), rows.iter().map(|r| r.source_bin_activity_fraction).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBinDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let source_bin_id = get_i64("sourceBinID")?;
        let source_bin_activity_fraction = get_f64("sourceBinActivityFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinDistributionRow {
                source_type_model_year_id: source_type_model_year_id.get(i).ok_or_else(|| null("sourceTypeModelYearID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                source_bin_activity_fraction: source_bin_activity_fraction.get(i).ok_or_else(|| null("sourceBinActivityFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceTypeAgeRow {
    fn table_name() -> &'static str { "SourceTypeAge" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("functioningACFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
            Series::new("functioningACFraction".into(), rows.iter().map(|r| r.functioning_ac_fraction).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeAge";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let functioning_ac_fraction = get_f64("functioningACFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceTypeAgeRow {
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                functioning_ac_fraction: functioning_ac_fraction.get(i).ok_or_else(|| null("functioningACFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceTypeModelYearRow {
    fn table_name() -> &'static str { "SourceTypeModelYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("ACPenetrationFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeModelYearID".into(), rows.iter().map(|r| r.source_type_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("ACPenetrationFraction".into(), rows.iter().map(|r| r.ac_penetration_fraction).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let ac_penetration_fraction = get_f64("ACPenetrationFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceTypeModelYearRow {
                source_type_model_year_id: source_type_model_year_id.get(i).ok_or_else(|| null("sourceTypeModelYearID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                ac_penetration_fraction: ac_penetration_fraction.get(i).ok_or_else(|| null("ACPenetrationFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for TemperatureAdjustmentRow {
    fn table_name() -> &'static str { "TemperatureAdjustment" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("tempAdjustTermA".into(), DataType::Float64),
            ("tempAdjustTermB".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("minModelYearID".into(), rows.iter().map(|r| r.min_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("maxModelYearID".into(), rows.iter().map(|r| r.max_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("tempAdjustTermA".into(), rows.iter().map(|r| r.temp_adjust_term_a).collect::<Vec<f64>>()).into(),
            Series::new("tempAdjustTermB".into(), rows.iter().map(|r| r.temp_adjust_term_b).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "TemperatureAdjustment";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let temp_adjust_term_a = get_f64("tempAdjustTermA")?;
        let temp_adjust_term_b = get_f64("tempAdjustTermB")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(TemperatureAdjustmentRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                min_model_year_id: min_model_year_id.get(i).ok_or_else(|| null("minModelYearID"))?,
                max_model_year_id: max_model_year_id.get(i).ok_or_else(|| null("maxModelYearID"))?,
                temp_adjust_term_a: temp_adjust_term_a.get(i).ok_or_else(|| null("tempAdjustTermA"))?,
                temp_adjust_term_b: temp_adjust_term_b.get(i).ok_or_else(|| null("tempAdjustTermB"))?,
            })
        }).collect()
    }
}

impl TableRow for YearRow {
    fn table_name() -> &'static str { "Year" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Year";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(YearRow {
                year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
            })
        }).collect()
    }
}

impl TableRow for ZoneMonthHourRow {
    fn table_name() -> &'static str { "ZoneMonthHour" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("temperature".into(), DataType::Float64),
            ("heatIndex".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
            Series::new("temperature".into(), rows.iter().map(|r| r.temperature).collect::<Vec<f64>>()).into(),
            Series::new("heatIndex".into(), rows.iter().map(|r| r.heat_index).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ZoneMonthHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        let heat_index = get_f64("heatIndex")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ZoneMonthHourRow {
                zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                temperature: temperature.get(i).ok_or_else(|| null("temperature"))?,
                heat_index: heat_index.get(i).ok_or_else(|| null("heatIndex"))?,
            })
        }).collect()
    }
}

impl TableRow for CriteriaRunningEmissionRow {
    fn table_name() -> &'static str { "MOVESWorkerOutput" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("stateID".into(), rows.iter().map(|r| r.state_id).collect::<Vec<i32>>()).into(),
            Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
            Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
            Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
            Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("emissionQuant".into(), rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let state_id = get_i32("stateID")?;
        let county_id = get_i32("countyID")?;
        let zone_id = get_i32("zoneID")?;
        let link_id = get_i32("linkID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(CriteriaRunningEmissionRow {
                state_id: state_id.get(i).ok_or_else(|| null("stateID"))?,
                county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
            })
        }).collect()
    }
}

// ===========================================================================
// Working tables — the intermediate MyISAM tables the SQL's "Processing"
// section builds. All are private to the module; each is the output of one
// CREC step and the input of a later one.
// ===========================================================================

/// CREC 1-a — `IMCoverageMergedUngrouped`: the summed inspection-and-
/// maintenance adjustment fraction per `(process, pollutant, modelYear,
/// fuelType, sourceType)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ImCoverageMerged {
    process_id: i32,
    pollutant_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    source_type_id: i32,
    im_adjust_fract: f64,
}

/// CREC 2-a — `CountyFuelAdjustment`: the GPA-blended fuel adjustment per
/// `(polProcess, modelYear, sourceType, fuelFormulation)`. The SQL table also
/// carries a `countyID` column (always the run's county); it is not used in
/// any join or in the output, so the port omits it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CountyFuelAdjustment {
    pol_process_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    fuel_formulation_id: i32,
    fuel_adjustment: f64,
}

/// CREC 2-b — `FuelSupplyWithFuelType`: a fuel formulation's market share for
/// the run's `(year, month)`, resolved to a fuel type. The SQL table also
/// carries a `countyID` column (the run's county); it is unused, so the port
/// omits it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelSupplyWithFuelType {
    year_id: i32,
    month_id: i32,
    fuel_formulation_id: i32,
    fuel_type_id: i32,
    market_share: f64,
}

/// CREC 2-b — `FuelSupplyAdjustment`: the market-share-weighted fuel
/// adjustment per `(year, month, polProcess, modelYear, sourceType,
/// fuelType)`. The SQL table also carries a `countyID` column (the run's
/// county); it is unused — the worker output stamps `countyID` from the run
/// context — so the port omits it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelSupplyAdjustment {
    year_id: i32,
    month_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    fuel_adjustment: f64,
}

/// CREC 3 — `METAdjustment`: the multiplicative temperature adjustment per
/// `(zone, month, hour, polProcess, fuelType, modelYear)`. The SQL table also
/// carries `minModelYearID`/`maxModelYearID` from `TemperatureAdjustment`;
/// CREC 7-a does not join on them, so the port omits them.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MetAdjustment {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    pol_process_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    temperature_adjustment: f64,
}

/// CREC 4-a — `ACOnFraction`: the clamped air-conditioning on-fraction per
/// `(zone, month, hour)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct AcOnFraction {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    ac_on_fraction: f64,
}

/// CREC 4-b — `ACActivityFraction`: the AC on-fraction weighted by penetration
/// and functioning fractions, per `(zone, month, hour, sourceType, modelYear)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct AcActivityFraction {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    ac_activity_fraction: f64,
}

/// CREC 4-c — `WeightedFullACAdjustment`: the full-AC adjustment paired to
/// each operating mode of the link's operating-mode distribution.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedFullAcAdjustment {
    source_type_id: i32,
    pol_process_id: i32,
    link_id: i32,
    hour_day_id: i32,
    op_mode_id: i32,
    weighted_full_ac_adjustment: f64,
}

/// CREC 4-d — `ACAdjustment`: the per-`(zone, month, hour, day, sourceType,
/// modelYear, polProcess, opMode)` air-conditioning emission-rate multiplier.
#[derive(Debug, Clone, Copy, PartialEq)]
struct AcAdjustment {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    day_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    op_mode_id: i32,
    ac_adjustment: f64,
}

/// CREC-5 — `SBWeightedEmissionRate`: the base emission rate weighted by
/// source-bin activity fraction and aggregated to source type.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SbWeightedEmissionRate {
    zone_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    op_mode_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CREC-6 — `FullyWeightedEmissionRate`: [`SbWeightedEmissionRate`] joined to
/// the operating-mode distribution, carrying the operating-mode fraction
/// forward (aggregation happens in CREC 7-c).
#[derive(Debug, Clone, Copy, PartialEq)]
struct FullyWeightedEmissionRate {
    link_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    hour_day_id: i32,
    op_mode_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
    op_mode_fraction: f64,
}

/// CREC 7-a — `TempAndACAdjustment`: the combined temperature × AC multiplier
/// per `(zone, polProcess, sourceType, modelYear, fuelType, month, hour, day,
/// opMode)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TempAndAcAdjustment {
    zone_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    month_id: i32,
    hour_id: i32,
    day_id: i32,
    op_mode_id: i32,
    temp_and_ac_adjustment: f64,
}

/// CREC 7-b — `FuelAdjustedRate`: [`FullyWeightedEmissionRate`] multiplied by
/// the fuel-supply adjustment (defaulting to `1.0` on a left-join miss).
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelAdjustedRate {
    link_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    month_id: i32,
    hour_day_id: i32,
    op_mode_id: i32,
    fuel_adjusted_rate: f64,
    fuel_adjusted_rate_im: f64,
    op_mode_fraction: f64,
}

/// CREC 7-c — `WeightedAndAdjustedEmissionRate`: the fuel-adjusted rate
/// multiplied by the temperature/AC adjustment and the operating-mode
/// fraction, then summed over operating mode. CREC 8's
/// `WeightedAndAdjustedEmissionRate2` has the same shape — see
/// `weighted_and_adjusted_emission_rate_2`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedAndAdjustedEmissionRate {
    link_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    hour_id: i32,
    day_id: i32,
    month_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CREC 9 — `SHO2`: the `SHO` activity re-keyed by model year (`year - ageID`)
/// with the `hourDayID` surrogate resolved to `(dayID, hourID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Sho2 {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    sho: f64,
}

/// CREC 9 — `WeightedAndAdjustedEmissionRate3`: CREC 8's output with
/// `polProcessID` split into `(pollutantID, processID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedAndAdjustedEmissionRate3 {
    link_id: i32,
    year_id: i32,
    pollutant_id: i32,
    process_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

// ===========================================================================
// Processing steps — one function per CREC step. Each takes the inputs (and
// any prior working tables) and returns the next working table as a `Vec`.
// ===========================================================================

/// CREC 1-a — `IMCoverageMergedUngrouped`.
///
/// `IMAdjustFract = Σ(imFactor × complianceFactor × 0.01)`, summed over the
/// `PollutantProcessMappedModelYear ⋈ PollutantProcessAssoc ⋈ IMFactor ⋈
/// AgeCategory ⋈ IMCoverage` join and grouped by `(processID, pollutantID,
/// modelYearID, fuelTypeID, sourceTypeID)`.
///
/// The `AgeCategory` join plus the `ppmy.modelYearID = year - ageID` filter
/// together require `IMFactor.ageGroupID` to be the age group of the age
/// `year - ppmy.modelYearID`; the port resolves that age once and compares.
/// This step is identical to the sibling start calculator's CSEC 1-a.
fn im_coverage_merged(inputs: &CriteriaRunningInputs, ctx: &RunContext) -> Vec<ImCoverageMerged> {
    // PollutantProcessAssoc lookup — polProcessID → (pollutantID, processID).
    let ppa: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
    // AgeCategory lookup — ageID → ageGroupID. `ageID` is the unique key.
    let age_group_by_age: HashMap<i32, i32> = inputs
        .age_category
        .iter()
        .map(|r| (r.age_id, r.age_group_id))
        .collect();
    // IMFactor indexed for the `(polProcessID, IMModelYearGroupID)` join.
    let mut imf_by_key: HashMap<(i32, i32), Vec<&ImFactorRow>> = HashMap::new();
    for imf in &inputs.im_factor {
        imf_by_key
            .entry((imf.pol_process_id, imf.im_model_year_group_id))
            .or_default()
            .push(imf);
    }
    // IMCoverage indexed for the five-column equality join; the model-year
    // range is filtered per matched row.
    let mut imc_by_key: HashMap<(i32, i32, i32, i32, i32), Vec<&ImCoverageRow>> = HashMap::new();
    for imc in &inputs.im_coverage {
        imc_by_key
            .entry((
                imc.pol_process_id,
                imc.inspect_freq,
                imc.test_standards_id,
                imc.source_type_id,
                imc.fuel_type_id,
            ))
            .or_default()
            .push(imc);
    }

    // GROUP BY (processID, pollutantID, modelYearID, fuelTypeID, sourceTypeID).
    let mut totals: HashMap<[i32; 5], f64> = HashMap::new();
    for ppmy in &inputs.pollutant_process_mapped_model_year {
        // INNER JOIN PollutantProcessAssoc USING (polProcessID).
        let Some(assoc) = ppa.get(&ppmy.pol_process_id) else {
            continue;
        };
        // INNER JOIN IMFactor ON (polProcessID, IMModelYearGroupID).
        let Some(imfs) = imf_by_key.get(&(ppmy.pol_process_id, ppmy.im_model_year_group_id)) else {
            continue;
        };
        // The single age whose model year is `ppmy.modelYearID` —
        // `ppmy.modelYearID = year - ageID`.
        let needed_age = ctx.year - ppmy.model_year_id;
        let Some(&needed_age_group) = age_group_by_age.get(&needed_age) else {
            continue;
        };
        for imf in imfs {
            // INNER JOIN AgeCategory ON (ageGroupID) AND modelYearID filter:
            // the matched age must be in `IMFactor.ageGroupID`.
            if imf.age_group_id != needed_age_group {
                continue;
            }
            // INNER JOIN IMCoverage ON (polProcessID, inspectFreq,
            // testStandardsID, sourceTypeID, fuelTypeID) AND model-year range.
            let Some(imcs) = imc_by_key.get(&(
                imf.pol_process_id,
                imf.inspect_freq,
                imf.test_standards_id,
                imf.source_type_id,
                imf.fuel_type_id,
            )) else {
                continue;
            };
            for imc in imcs {
                if ppmy.model_year_id < imc.beg_model_year_id
                    || ppmy.model_year_id > imc.end_model_year_id
                {
                    continue;
                }
                // key: [process, pollutant, modelYear, fuelType, sourceType].
                *totals
                    .entry([
                        assoc.process_id,
                        assoc.pollutant_id,
                        ppmy.model_year_id,
                        imf.fuel_type_id,
                        imc.source_type_id,
                    ])
                    .or_default() += imf.im_factor * imc.compliance_factor * 0.01;
            }
        }
    }

    totals
        .into_iter()
        .map(|(k, im_adjust_fract)| ImCoverageMerged {
            process_id: k[0],
            pollutant_id: k[1],
            model_year_id: k[2],
            fuel_type_id: k[3],
            source_type_id: k[4],
            im_adjust_fract,
        })
        .collect()
}

/// CREC 2-a — `CountyFuelAdjustment`.
///
/// The cartesian product `criteriaRatio × County` (the SQL's
/// `criteriaRatio INNER JOIN County` has no `ON` clause). The fuel adjustment
/// is `ratio + GPAFract × (ratioGPA − ratio)`; the polProcess, model year,
/// source type and fuel formulation pass straight through from `criteriaRatio`.
fn county_fuel_adjustment(inputs: &CriteriaRunningInputs) -> Vec<CountyFuelAdjustment> {
    let mut out: Vec<CountyFuelAdjustment> = Vec::new();
    for cr in &inputs.criteria_ratio {
        for county in &inputs.county {
            out.push(CountyFuelAdjustment {
                pol_process_id: cr.pol_process_id,
                model_year_id: cr.model_year_id,
                source_type_id: cr.source_type_id,
                fuel_formulation_id: cr.fuel_formulation_id,
                fuel_adjustment: cr.ratio + county.gpa_fract * (cr.ratio_gpa - cr.ratio),
            });
        }
    }
    out
}

/// CREC 2-b — `FuelSupplyWithFuelType`.
///
/// `FuelSupply ⋈ FuelFormulation ⋈ FuelSubtype ⋈ MonthOfAnyYear ⋈ Year`,
/// resolving each fuel supply to a fuel type and stamping the run's year and
/// month. `Year` is extract-filtered to the run's year; the SQL's
/// `WHERE y.yearID = ##context.year##` is reapplied for fidelity.
fn fuel_supply_with_fuel_type(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
) -> Vec<FuelSupplyWithFuelType> {
    // fuelFormulationID → fuelSubtypeID; fuelFormulationID is the unique key.
    let subtype_of_formulation: HashMap<i32, i32> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff.fuel_subtype_id))
        .collect();
    // fuelSubtypeID → fuelTypeID; fuelSubtypeID is the unique key.
    let fuel_type_of_subtype: HashMap<i32, i32> = inputs
        .fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst.fuel_type_id))
        .collect();
    // MonthOfAnyYear indexed by month group — a group spans several months.
    let mut months_by_group: HashMap<i32, Vec<&MonthOfAnyYearRow>> = HashMap::new();
    for may in &inputs.month_of_any_year {
        months_by_group
            .entry(may.month_group_id)
            .or_default()
            .push(may);
    }
    // Year indexed by fuel year.
    let mut years_by_fuel_year: HashMap<i32, Vec<&YearRow>> = HashMap::new();
    for y in &inputs.year {
        years_by_fuel_year
            .entry(y.fuel_year_id)
            .or_default()
            .push(y);
    }

    let mut out: Vec<FuelSupplyWithFuelType> = Vec::new();
    for fs in &inputs.fuel_supply {
        // INNER JOIN FuelFormulation USING (fuelFormulationID).
        let Some(&subtype_id) = subtype_of_formulation.get(&fs.fuel_formulation_id) else {
            continue;
        };
        // INNER JOIN FuelSubtype USING (fuelSubtypeID).
        let Some(&fuel_type_id) = fuel_type_of_subtype.get(&subtype_id) else {
            continue;
        };
        // INNER JOIN MonthOfAnyYear ON (monthGroupID).
        let Some(mays) = months_by_group.get(&fs.month_group_id) else {
            continue;
        };
        // INNER JOIN Year ON (fuelYearID).
        let Some(years) = years_by_fuel_year.get(&fs.fuel_year_id) else {
            continue;
        };
        for may in mays {
            for y in years {
                // WHERE y.yearID = ##context.year##.
                if y.year_id != ctx.year {
                    continue;
                }
                out.push(FuelSupplyWithFuelType {
                    year_id: y.year_id,
                    month_id: may.month_id,
                    fuel_formulation_id: fs.fuel_formulation_id,
                    fuel_type_id,
                    market_share: fs.market_share,
                });
            }
        }
    }
    out
}

/// CREC 2-b — `FuelSupplyAdjustment`.
///
/// `fuelAdjustment = Σ(fuelAdjustment × marketShare)`, summed over
/// `CountyFuelAdjustment ⋈ FuelSupplyWithFuelType` (joined on
/// `fuelFormulationID`) and grouped by `(year, month, polProcess, modelYear,
/// sourceType, fuelType)`.
fn fuel_supply_adjustment(
    county_fuel: &[CountyFuelAdjustment],
    fuel_supply_ft: &[FuelSupplyWithFuelType],
) -> Vec<FuelSupplyAdjustment> {
    // FuelSupplyWithFuelType indexed for the `fuelFormulationID` join.
    let mut fsft_by_formulation: HashMap<i32, Vec<&FuelSupplyWithFuelType>> = HashMap::new();
    for fsft in fuel_supply_ft {
        fsft_by_formulation
            .entry(fsft.fuel_formulation_id)
            .or_default()
            .push(fsft);
    }

    // GROUP BY (yearID, monthID, polProcessID, modelYearID, sourceTypeID,
    // fuelTypeID).
    let mut totals: HashMap<[i32; 6], f64> = HashMap::new();
    for cfa in county_fuel {
        // INNER JOIN FuelSupplyWithFuelType ON (fuelFormulationID).
        let Some(fsfts) = fsft_by_formulation.get(&cfa.fuel_formulation_id) else {
            continue;
        };
        for fsft in fsfts {
            *totals
                .entry([
                    fsft.year_id,
                    fsft.month_id,
                    cfa.pol_process_id,
                    cfa.model_year_id,
                    cfa.source_type_id,
                    fsft.fuel_type_id,
                ])
                .or_default() += cfa.fuel_adjustment * fsft.market_share;
        }
    }

    totals
        .into_iter()
        .map(|(k, fuel_adjustment)| FuelSupplyAdjustment {
            year_id: k[0],
            month_id: k[1],
            pol_process_id: k[2],
            model_year_id: k[3],
            source_type_id: k[4],
            fuel_type_id: k[5],
            fuel_adjustment,
        })
        .collect()
}

/// CREC 3 — `METAdjustment`.
///
/// The cartesian product `ZoneMonthHour × TemperatureAdjustment × ModelYear`,
/// keeping `TemperatureAdjustment` rows whose `polProcessID` resolves to a
/// Running Exhaust pair and `ModelYear`s inside the row's
/// `[minModelYearID, maxModelYearID]` band. The multiplicative adjustment is
/// `1 + d × (tempAdjustTermA + d × tempAdjustTermB)` with `d = temperature − 75`.
fn met_adjustment(inputs: &CriteriaRunningInputs) -> Vec<MetAdjustment> {
    // PollutantProcessAssoc lookup — a TemperatureAdjustment row is kept only
    // if its polProcessID resolves to a Running Exhaust (process 1) pair.
    let ppa: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();

    let mut out: Vec<MetAdjustment> = Vec::new();
    for zmh in &inputs.zone_month_hour {
        for ta in &inputs.temperature_adjustment {
            // INNER JOIN PollutantProcessAssoc ON (polProcessID), processID = 1.
            let Some(assoc) = ppa.get(&ta.pol_process_id) else {
                continue;
            };
            if assoc.process_id != i32::from(RUNNING_EXHAUST_PROCESS_ID) {
                continue;
            }
            let delta = zmh.temperature - RUNNING_TEMP_REFERENCE_F;
            let temperature_adjustment =
                1.0 + delta * (ta.temp_adjust_term_a + delta * ta.temp_adjust_term_b);
            for my in &inputs.model_year {
                // JOIN ModelYear ON modelYearID BETWEEN minModelYearID AND
                // maxModelYearID.
                if my.model_year_id < ta.min_model_year_id
                    || my.model_year_id > ta.max_model_year_id
                {
                    continue;
                }
                out.push(MetAdjustment {
                    zone_id: zmh.zone_id,
                    month_id: zmh.month_id,
                    hour_id: zmh.hour_id,
                    pol_process_id: ta.pol_process_id,
                    fuel_type_id: ta.fuel_type_id,
                    model_year_id: my.model_year_id,
                    temperature_adjustment,
                });
            }
        }
    }
    out
}

/// CREC 4-a — `ACOnFraction`.
///
/// `ACOnFraction = LEAST(GREATEST(A + h × (B + C × h), 0), 1)` — the
/// AC-activity polynomial in `heatIndex`, clamped to `[0, 1]` — over
/// `ZoneMonthHour ⋈ MonthOfAnyYear ⋈ MonthGroupHour`.
fn ac_on_fraction(inputs: &CriteriaRunningInputs) -> Vec<AcOnFraction> {
    // MonthOfAnyYear lookup — monthID → monthGroupID. `monthID` is unique.
    let month_group_by_month: HashMap<i32, i32> = inputs
        .month_of_any_year
        .iter()
        .map(|may| (may.month_id, may.month_group_id))
        .collect();
    // MonthGroupHour indexed for the `(monthGroupID, hourID)` join.
    let mgh_by_key: HashMap<(i32, i32), &MonthGroupHourRow> = inputs
        .month_group_hour
        .iter()
        .map(|mgh| ((mgh.month_group_id, mgh.hour_id), mgh))
        .collect();

    let mut out: Vec<AcOnFraction> = Vec::new();
    for zmh in &inputs.zone_month_hour {
        // INNER JOIN MonthOfAnyYear ON (monthID).
        let Some(&month_group_id) = month_group_by_month.get(&zmh.month_id) else {
            continue;
        };
        // INNER JOIN MonthGroupHour ON (monthGroupID, hourID).
        let Some(mgh) = mgh_by_key.get(&(month_group_id, zmh.hour_id)) else {
            continue;
        };
        let h = zmh.heat_index;
        let raw =
            mgh.ac_activity_term_a + h * (mgh.ac_activity_term_b + mgh.ac_activity_term_c * h);
        out.push(AcOnFraction {
            zone_id: zmh.zone_id,
            month_id: zmh.month_id,
            hour_id: zmh.hour_id,
            // LEAST(GREATEST(raw, 0), 1.0) — `heatIndex` is a finite
            // temperature, so `clamp` matches the SQL on every input.
            ac_on_fraction: raw.clamp(0.0, 1.0),
        });
    }
    out
}

/// CREC 4-b — `ACActivityFraction`.
///
/// `ACActivityFraction = ACOnFraction × ACPenetrationFraction ×
/// functioningACFraction`, over the cartesian product
/// `ACOnFraction × SourceTypeModelYear` joined to `SourceTypeAge` on
/// `(sourceTypeID, ageID = year − modelYearID)`.
fn ac_activity_fraction(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
    ac_on: &[AcOnFraction],
) -> Vec<AcActivityFraction> {
    // SourceTypeAge indexed for the `(sourceTypeID, ageID)` join.
    let sta_by_key: HashMap<(i32, i32), &SourceTypeAgeRow> = inputs
        .source_type_age
        .iter()
        .map(|sta| ((sta.source_type_id, sta.age_id), sta))
        .collect();

    let mut out: Vec<AcActivityFraction> = Vec::new();
    for acof in ac_on {
        for stmy in &inputs.source_type_model_year {
            // INNER JOIN SourceTypeAge ON (sourceTypeID, ageID = year - modelYear).
            let age_id = ctx.year - stmy.model_year_id;
            let Some(sta) = sta_by_key.get(&(stmy.source_type_id, age_id)) else {
                continue;
            };
            out.push(AcActivityFraction {
                zone_id: acof.zone_id,
                month_id: acof.month_id,
                hour_id: acof.hour_id,
                source_type_id: stmy.source_type_id,
                model_year_id: stmy.model_year_id,
                ac_activity_fraction: acof.ac_on_fraction
                    * stmy.ac_penetration_fraction
                    * sta.functioning_ac_fraction,
            });
        }
    }
    out
}

/// CREC 4-c — `WeightedFullACAdjustment`.
///
/// `OpModeDistribution ⋈ FullACAdjustment` on `(sourceTypeID, polProcessID,
/// opModeID)`, carrying the `fullACAdjustment` per operating mode. The SQL
/// once weighted it by `opModeFraction` (the commented-out variant); the
/// active statement copies the unweighted value, so the port does too. `linkID`
/// is the run's link (the `OpModeDistribution` extract filtered it).
fn weighted_full_ac_adjustment(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
) -> Vec<WeightedFullAcAdjustment> {
    // FullACAdjustment indexed for the three-column join.
    let mut faca_by_key: HashMap<(i32, i32, i32), Vec<&FullAcAdjustmentRow>> = HashMap::new();
    for faca in &inputs.full_ac_adjustment {
        faca_by_key
            .entry((faca.source_type_id, faca.pol_process_id, faca.op_mode_id))
            .or_default()
            .push(faca);
    }

    let mut out: Vec<WeightedFullAcAdjustment> = Vec::new();
    for omd in &inputs.op_mode_distribution {
        // INNER JOIN FullACAdjustment ON (sourceTypeID, polProcessID, opModeID).
        let Some(facas) =
            faca_by_key.get(&(omd.source_type_id, omd.pol_process_id, omd.op_mode_id))
        else {
            continue;
        };
        for faca in facas {
            out.push(WeightedFullAcAdjustment {
                source_type_id: omd.source_type_id,
                pol_process_id: omd.pol_process_id,
                link_id: ctx.link_id,
                hour_day_id: omd.hour_day_id,
                op_mode_id: omd.op_mode_id,
                weighted_full_ac_adjustment: faca.full_ac_adjustment,
            });
        }
    }
    out
}

/// CREC 4-d — `ACAdjustment`.
///
/// `ACAdjustment = 1 + (weightedFullACAdjustment − 1) × ACActivityFraction`,
/// over `ACActivityFraction ⋈ Link (on zoneID) ⋈ HourDay (on hourID) ⋈
/// WeightedFullACAdjustment (on sourceTypeID, linkID, hourDayID)`.
fn ac_adjustment(
    inputs: &CriteriaRunningInputs,
    ac_activity: &[AcActivityFraction],
    weighted_full_ac: &[WeightedFullAcAdjustment],
) -> Vec<AcAdjustment> {
    // Link indexed by zone — the join keys on zoneID.
    let mut links_by_zone: HashMap<i32, Vec<&LinkRow>> = HashMap::new();
    for l in &inputs.link {
        links_by_zone.entry(l.zone_id).or_default().push(l);
    }
    // HourDay indexed by hour — an hour spans several day types.
    let mut hour_days_by_hour: HashMap<i32, Vec<&HourDayRow>> = HashMap::new();
    for hd in &inputs.hour_day {
        hour_days_by_hour.entry(hd.hour_id).or_default().push(hd);
    }
    // WeightedFullACAdjustment indexed for the three-column join.
    let mut wfaca_by_key: HashMap<(i32, i32, i32), Vec<&WeightedFullAcAdjustment>> = HashMap::new();
    for wfaca in weighted_full_ac {
        wfaca_by_key
            .entry((wfaca.source_type_id, wfaca.link_id, wfaca.hour_day_id))
            .or_default()
            .push(wfaca);
    }

    let mut out: Vec<AcAdjustment> = Vec::new();
    for acaf in ac_activity {
        // INNER JOIN Link ON (acaf.zoneID = l.zoneID).
        let Some(links) = links_by_zone.get(&acaf.zone_id) else {
            continue;
        };
        // INNER JOIN HourDay ON (hd.hourID = acaf.hourID).
        let Some(hour_days) = hour_days_by_hour.get(&acaf.hour_id) else {
            continue;
        };
        for l in links {
            for hd in hour_days {
                // INNER JOIN WeightedFullACAdjustment ON (sourceTypeID, linkID,
                // hourDayID).
                let Some(wfacas) =
                    wfaca_by_key.get(&(acaf.source_type_id, l.link_id, hd.hour_day_id))
                else {
                    continue;
                };
                for wfaca in wfacas {
                    out.push(AcAdjustment {
                        zone_id: acaf.zone_id,
                        month_id: acaf.month_id,
                        hour_id: hd.hour_id,
                        day_id: hd.day_id,
                        source_type_id: acaf.source_type_id,
                        model_year_id: acaf.model_year_id,
                        pol_process_id: wfaca.pol_process_id,
                        op_mode_id: wfaca.op_mode_id,
                        ac_adjustment: 1.0
                            + (wfaca.weighted_full_ac_adjustment - 1.0) * acaf.ac_activity_fraction,
                    });
                }
            }
        }
    }
    out
}

/// CREC-5 — `SBWeightedEmissionRate`.
///
/// `meanBaseRate = Σ(sourceBinActivityFraction × meanBaseRate)`, summed over
/// `EmissionRateByAge ⋈ AgeCategory ⋈ SourceTypeModelYear (on modelYearID =
/// year − ageID) ⋈ SourceBinDistribution ⋈ SourceBin` and grouped by
/// `(polProcess, sourceType, ageID, fuelType, opMode)`.
fn sb_weighted_emission_rate(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
) -> Vec<SbWeightedEmissionRate> {
    // AgeCategory indexed by age group — each group holds several ages.
    let mut ages_by_group: HashMap<i32, Vec<&AgeCategoryRow>> = HashMap::new();
    for age in &inputs.age_category {
        ages_by_group.entry(age.age_group_id).or_default().push(age);
    }
    // SourceTypeModelYear indexed by model year — the join keys on modelYearID.
    let mut stmy_by_model_year: HashMap<i32, Vec<&SourceTypeModelYearRow>> = HashMap::new();
    for stmy in &inputs.source_type_model_year {
        stmy_by_model_year
            .entry(stmy.model_year_id)
            .or_default()
            .push(stmy);
    }
    // SourceBinDistribution indexed for the three-column join.
    let mut sbd_by_key: HashMap<(i32, i32, i64), Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        sbd_by_key
            .entry((
                sbd.source_type_model_year_id,
                sbd.pol_process_id,
                sbd.source_bin_id,
            ))
            .or_default()
            .push(sbd);
    }
    // SourceBin lookup — sourceBinID → fuelTypeID.
    let fuel_type_of_bin: HashMap<i64, i32> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb.fuel_type_id))
        .collect();

    // GROUP BY (polProcessID, sourceTypeID, ageID, fuelTypeID, opModeID); the
    // model year (`year - ageID`) is carried for the SELECT.
    let mut totals: HashMap<[i32; 5], (f64, f64, i32)> = HashMap::new();
    for erim in &inputs.emission_rate_by_age {
        // INNER JOIN AgeCategory ON (ageGroupID).
        let Some(ages) = ages_by_group.get(&erim.age_group_id) else {
            continue;
        };
        for age in ages {
            // INNER JOIN SourceTypeModelYear ON (modelYearID = year - ageID).
            let model_year_id = ctx.year - age.age_id;
            let Some(stmys) = stmy_by_model_year.get(&model_year_id) else {
                continue;
            };
            for stmy in stmys {
                // INNER JOIN SourceBinDistribution ON (sourceTypeModelYearID,
                // polProcessID, sourceBinID).
                let Some(sbds) = sbd_by_key.get(&(
                    stmy.source_type_model_year_id,
                    erim.pol_process_id,
                    erim.source_bin_id,
                )) else {
                    continue;
                };
                // INNER JOIN SourceBin USING (sourceBinID).
                let Some(&fuel_type_id) = fuel_type_of_bin.get(&erim.source_bin_id) else {
                    continue;
                };
                for sbd in sbds {
                    let entry = totals
                        .entry([
                            erim.pol_process_id,
                            stmy.source_type_id,
                            age.age_id,
                            fuel_type_id,
                            erim.op_mode_id,
                        ])
                        .or_insert((0.0, 0.0, model_year_id));
                    entry.0 += sbd.source_bin_activity_fraction * erim.mean_base_rate;
                    entry.1 += sbd.source_bin_activity_fraction * erim.mean_base_rate_im;
                }
            }
        }
    }

    totals
        .into_iter()
        .map(
            |(k, (mean_base_rate, mean_base_rate_im, model_year_id))| SbWeightedEmissionRate {
                zone_id: ctx.zone_id,
                year_id: ctx.year,
                pol_process_id: k[0],
                source_type_id: k[1],
                model_year_id,
                fuel_type_id: k[3],
                op_mode_id: k[4],
                mean_base_rate,
                mean_base_rate_im,
            },
        )
        .collect()
}

/// CREC-6 — `FullyWeightedEmissionRate`.
///
/// `SBWeightedEmissionRate ⋈ OpModeDistribution` using `(polProcessID,
/// sourceTypeID, opModeID)`, expanding each rate across the distribution's
/// hour-days and carrying `opModeFraction` forward. `linkID` is the run's link.
fn fully_weighted_emission_rate(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
    sb_weighted: &[SbWeightedEmissionRate],
) -> Vec<FullyWeightedEmissionRate> {
    // OpModeDistribution indexed for the `USING (polProcessID, sourceTypeID,
    // opModeID)` join.
    let mut omd_by_key: HashMap<(i32, i32, i32), Vec<&OpModeDistributionRow>> = HashMap::new();
    for omd in &inputs.op_mode_distribution {
        omd_by_key
            .entry((omd.pol_process_id, omd.source_type_id, omd.op_mode_id))
            .or_default()
            .push(omd);
    }

    let mut out: Vec<FullyWeightedEmissionRate> = Vec::new();
    for sbwer in sb_weighted {
        let Some(omds) =
            omd_by_key.get(&(sbwer.pol_process_id, sbwer.source_type_id, sbwer.op_mode_id))
        else {
            continue;
        };
        for omd in omds {
            out.push(FullyWeightedEmissionRate {
                link_id: ctx.link_id,
                year_id: sbwer.year_id,
                pol_process_id: sbwer.pol_process_id,
                source_type_id: sbwer.source_type_id,
                model_year_id: sbwer.model_year_id,
                fuel_type_id: sbwer.fuel_type_id,
                hour_day_id: omd.hour_day_id,
                op_mode_id: sbwer.op_mode_id,
                mean_base_rate: sbwer.mean_base_rate,
                mean_base_rate_im: sbwer.mean_base_rate_im,
                op_mode_fraction: omd.op_mode_fraction,
            });
        }
    }
    out
}

/// CREC 7-a — `TempAndACAdjustment`.
///
/// `tempAndACAdjustment = temperatureAdjustment × ACAdjustment`, over
/// `METAdjustment ⋈ ACAdjustment` on `(zoneID, monthID, hourID, polProcessID,
/// modelYearID)`. The source type, day and operating mode come from
/// `ACAdjustment`; the fuel type from `METAdjustment`.
fn temp_and_ac_adjustment(
    met_adj: &[MetAdjustment],
    ac_adj: &[AcAdjustment],
) -> Vec<TempAndAcAdjustment> {
    // ACAdjustment indexed for the five-column join.
    let mut aca_by_key: HashMap<[i32; 5], Vec<&AcAdjustment>> = HashMap::new();
    for aca in ac_adj {
        aca_by_key
            .entry([
                aca.zone_id,
                aca.month_id,
                aca.hour_id,
                aca.pol_process_id,
                aca.model_year_id,
            ])
            .or_default()
            .push(aca);
    }

    let mut out: Vec<TempAndAcAdjustment> = Vec::new();
    for ma in met_adj {
        let Some(acas) = aca_by_key.get(&[
            ma.zone_id,
            ma.month_id,
            ma.hour_id,
            ma.pol_process_id,
            ma.model_year_id,
        ]) else {
            continue;
        };
        for aca in acas {
            out.push(TempAndAcAdjustment {
                zone_id: ma.zone_id,
                pol_process_id: ma.pol_process_id,
                source_type_id: aca.source_type_id,
                model_year_id: aca.model_year_id,
                fuel_type_id: ma.fuel_type_id,
                month_id: ma.month_id,
                hour_id: ma.hour_id,
                day_id: aca.day_id,
                op_mode_id: aca.op_mode_id,
                temp_and_ac_adjustment: ma.temperature_adjustment * aca.ac_adjustment,
            });
        }
    }
    out
}

/// CREC 7-b — `FuelAdjustedRate`.
///
/// `fuelAdjustedRate = meanBaseRate × ifnull(fuelAdjustment, 1.0)`, over the
/// cartesian product `MonthOfAnyYear × FullyWeightedEmissionRate` left-joined
/// to `FuelSupplyAdjustment` on `(yearID, polProcessID, modelYearID,
/// sourceTypeID, fuelTypeID, monthID)`. A row with no fuel-supply match keeps
/// `fuelAdjustment = 1.0`.
fn fuel_adjusted_rate(
    inputs: &CriteriaRunningInputs,
    fully_weighted: &[FullyWeightedEmissionRate],
    fuel_supply_adj: &[FuelSupplyAdjustment],
) -> Vec<FuelAdjustedRate> {
    // FuelSupplyAdjustment indexed for the six-column left join.
    let mut fsa_by_key: HashMap<[i32; 6], Vec<&FuelSupplyAdjustment>> = HashMap::new();
    for fsa in fuel_supply_adj {
        fsa_by_key
            .entry([
                fsa.year_id,
                fsa.pol_process_id,
                fsa.model_year_id,
                fsa.source_type_id,
                fsa.fuel_type_id,
                fsa.month_id,
            ])
            .or_default()
            .push(fsa);
    }

    let mut out: Vec<FuelAdjustedRate> = Vec::new();
    for may in &inputs.month_of_any_year {
        for fwer in fully_weighted {
            // LEFT OUTER JOIN FuelSupplyAdjustment.
            let matches = fsa_by_key.get(&[
                fwer.year_id,
                fwer.pol_process_id,
                fwer.model_year_id,
                fwer.source_type_id,
                fwer.fuel_type_id,
                may.month_id,
            ]);
            let mut emit = |fuel_adjustment: f64| {
                out.push(FuelAdjustedRate {
                    link_id: fwer.link_id,
                    year_id: fwer.year_id,
                    pol_process_id: fwer.pol_process_id,
                    source_type_id: fwer.source_type_id,
                    model_year_id: fwer.model_year_id,
                    fuel_type_id: fwer.fuel_type_id,
                    month_id: may.month_id,
                    hour_day_id: fwer.hour_day_id,
                    op_mode_id: fwer.op_mode_id,
                    fuel_adjusted_rate: fwer.mean_base_rate * fuel_adjustment,
                    fuel_adjusted_rate_im: fwer.mean_base_rate_im * fuel_adjustment,
                    op_mode_fraction: fwer.op_mode_fraction,
                });
            };
            match matches {
                // LEFT JOIN miss — ifnull(NULL, 1.0) gives 1.0.
                None => emit(1.0),
                Some(fsas) => {
                    for fsa in fsas {
                        emit(fsa.fuel_adjustment);
                    }
                }
            }
        }
    }
    out
}

/// CREC 7-c — `WeightedAndAdjustedEmissionRate`.
///
/// `meanBaseRate = Σ(fuelAdjustedRate × tempAndACAdjustment × opModeFraction)`,
/// summed over `FuelAdjustedRate ⋈ Link ⋈ HourDay ⋈ TempAndACAdjustment` and
/// grouped by `(linkID, yearID, polProcessID, sourceTypeID, modelYearID,
/// fuelTypeID, hourID, dayID, monthID)` — collapsing the operating-mode
/// dimension.
fn weighted_and_adjusted_emission_rate(
    inputs: &CriteriaRunningInputs,
    fuel_adjusted: &[FuelAdjustedRate],
    temp_and_ac: &[TempAndAcAdjustment],
) -> Vec<WeightedAndAdjustedEmissionRate> {
    // Link keyed by its `linkID` primary key.
    let link_by_id: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();
    // HourDay keyed by its `hourDayID` primary key.
    let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    // TempAndACAdjustment indexed for the nine-column join.
    let mut taca_by_key: HashMap<[i32; 9], Vec<&TempAndAcAdjustment>> = HashMap::new();
    for taca in temp_and_ac {
        taca_by_key
            .entry([
                taca.zone_id,
                taca.pol_process_id,
                taca.source_type_id,
                taca.model_year_id,
                taca.fuel_type_id,
                taca.month_id,
                taca.day_id,
                taca.hour_id,
                taca.op_mode_id,
            ])
            .or_default()
            .push(taca);
    }

    // GROUP BY (linkID, yearID, polProcessID, sourceTypeID, modelYearID,
    // fuelTypeID, hourID, dayID, monthID).
    let mut totals: HashMap<[i32; 9], (f64, f64)> = HashMap::new();
    for far in fuel_adjusted {
        // INNER JOIN Link USING (linkID).
        let Some(l) = link_by_id.get(&far.link_id) else {
            continue;
        };
        // INNER JOIN HourDay USING (hourDayID).
        let Some(hd) = hour_day_by_id.get(&far.hour_day_id) else {
            continue;
        };
        // INNER JOIN TempAndACAdjustment ON (zoneID = l.zoneID, polProcessID,
        // sourceTypeID, modelYearID, fuelTypeID, monthID, dayID, hourID,
        // opModeID).
        let Some(tacas) = taca_by_key.get(&[
            l.zone_id,
            far.pol_process_id,
            far.source_type_id,
            far.model_year_id,
            far.fuel_type_id,
            far.month_id,
            hd.day_id,
            hd.hour_id,
            far.op_mode_id,
        ]) else {
            continue;
        };
        for taca in tacas {
            let entry = totals
                .entry([
                    far.link_id,
                    far.year_id,
                    far.pol_process_id,
                    far.source_type_id,
                    far.model_year_id,
                    far.fuel_type_id,
                    hd.hour_id,
                    hd.day_id,
                    far.month_id,
                ])
                .or_insert((0.0, 0.0));
            entry.0 += far.fuel_adjusted_rate * taca.temp_and_ac_adjustment * far.op_mode_fraction;
            entry.1 +=
                far.fuel_adjusted_rate_im * taca.temp_and_ac_adjustment * far.op_mode_fraction;
        }
    }

    totals
        .into_iter()
        .map(
            |(k, (mean_base_rate, mean_base_rate_im))| WeightedAndAdjustedEmissionRate {
                link_id: k[0],
                year_id: k[1],
                pol_process_id: k[2],
                source_type_id: k[3],
                model_year_id: k[4],
                fuel_type_id: k[5],
                hour_id: k[6],
                day_id: k[7],
                month_id: k[8],
                mean_base_rate,
                mean_base_rate_im,
            },
        )
        .collect()
}

/// CREC 8 — `WeightedAndAdjustedEmissionRate2`: the disabled NOx-humidity step.
///
/// The pinned script splits CREC 7-c's output into a NOx branch
/// (`WeightedAndAdjustedEmissionRate2_TEMP1`, the `polProcessID = 301` rows
/// inner-joined to `Link`, `ZoneMonthHour` and `FuelType`) and a non-NOx
/// branch (`_TEMP2`), then `UNION`s them. Neither branch applies a humidity
/// multiply — Bug 431 left the join structure but removed the correction — so
/// the step is a structural pass-through; see the [module docs](self).
///
/// This port reproduces the TEMP1 join filter (which, under the extract's
/// referential integrity, drops nothing) and the `301` / non-`301` partition,
/// then concatenates. The two partitions are disjoint on `polProcessID`, so
/// the SQL `UNION`'s deduplication is a no-op.
fn weighted_and_adjusted_emission_rate_2(
    inputs: &CriteriaRunningInputs,
    weighted_adjusted: &[WeightedAndAdjustedEmissionRate],
) -> Vec<WeightedAndAdjustedEmissionRate> {
    // Link keyed by its `linkID` primary key — TEMP1 resolves the zone here.
    let link_by_id: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();
    // ZoneMonthHour keyed by (zoneID, monthID, hourID) for the TEMP1 join.
    let zmh_keys: HashSet<(i32, i32, i32)> = inputs
        .zone_month_hour
        .iter()
        .map(|zmh| (zmh.zone_id, zmh.month_id, zmh.hour_id))
        .collect();
    // FuelType keys for the TEMP1 join.
    let fuel_type_keys: HashSet<i32> = inputs.fuel_type.iter().map(|ft| ft.fuel_type_id).collect();

    let mut out: Vec<WeightedAndAdjustedEmissionRate> = Vec::new();
    for waer in weighted_adjusted {
        if waer.pol_process_id == NOX_RUNNING_POL_PROCESS_ID {
            // TEMP1 — INNER JOIN Link, ZoneMonthHour (on the link's zone) and
            // FuelType. The rate passes through unchanged.
            let Some(l) = link_by_id.get(&waer.link_id) else {
                continue;
            };
            if !zmh_keys.contains(&(l.zone_id, waer.month_id, waer.hour_id)) {
                continue;
            }
            if !fuel_type_keys.contains(&waer.fuel_type_id) {
                continue;
            }
            out.push(*waer);
        } else {
            // TEMP2 — every other polProcessID, copied directly.
            out.push(*waer);
        }
    }
    out
}

/// CREC 9 — `SHO2`.
///
/// Re-keys the `SHO` activity by model year (`yearID − ageID`) and resolves
/// the `hourDayID` surrogate to `(dayID, hourID)` through `HourDay`.
fn build_sho2(inputs: &CriteriaRunningInputs) -> Vec<Sho2> {
    // HourDay keyed by its `hourDayID` primary key.
    let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();

    let mut out: Vec<Sho2> = Vec::new();
    for sho in &inputs.sho {
        // INNER JOIN HourDay USING (hourDayID).
        let Some(hd) = hour_day_by_id.get(&sho.hour_day_id) else {
            continue;
        };
        out.push(Sho2 {
            year_id: sho.year_id,
            month_id: sho.month_id,
            day_id: hd.day_id,
            hour_id: hd.hour_id,
            source_type_id: sho.source_type_id,
            model_year_id: sho.year_id - sho.age_id,
            sho: sho.sho,
        });
    }
    out
}

/// CREC 9 — `WeightedAndAdjustedEmissionRate3`.
///
/// Splits CREC 8's `polProcessID` into `(pollutantID, processID)` through
/// `PollutantProcessAssoc`.
fn weighted_and_adjusted_emission_rate_3(
    inputs: &CriteriaRunningInputs,
    weighted_adjusted_2: &[WeightedAndAdjustedEmissionRate],
) -> Vec<WeightedAndAdjustedEmissionRate3> {
    // PollutantProcessAssoc lookup — polProcessID → (pollutantID, processID).
    let ppa: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();

    let mut out: Vec<WeightedAndAdjustedEmissionRate3> = Vec::new();
    for waer in weighted_adjusted_2 {
        // INNER JOIN PollutantProcessAssoc USING (polProcessID).
        let Some(assoc) = ppa.get(&waer.pol_process_id) else {
            continue;
        };
        out.push(WeightedAndAdjustedEmissionRate3 {
            link_id: waer.link_id,
            year_id: waer.year_id,
            pollutant_id: assoc.pollutant_id,
            process_id: assoc.process_id,
            source_type_id: waer.source_type_id,
            fuel_type_id: waer.fuel_type_id,
            model_year_id: waer.model_year_id,
            month_id: waer.month_id,
            day_id: waer.day_id,
            hour_id: waer.hour_id,
            mean_base_rate: waer.mean_base_rate,
            mean_base_rate_im: waer.mean_base_rate_im,
        });
    }
    out
}

/// CREC 9 — `SHO3` plus `MOVESWorkerOutput` and the I/M `update`.
///
/// `SHO2 ⋈ WeightedAndAdjustedEmissionRate3 ⋈ Link` produces `emissionQuant =
/// meanBaseRate × SHO` and `emissionQuantIM = meanBaseRateIM × SHO`; the I/M
/// `update` then blends them per matched `IMCoverageMergedUngrouped` row:
/// `max(emissionQuantIM × IMAdjustFract + emissionQuant × (1 −
/// IMAdjustFract), 0)`. A row with no I/M match keeps `emissionQuant`.
fn assemble_emission_output(
    inputs: &CriteriaRunningInputs,
    ctx: &RunContext,
    sho2: &[Sho2],
    waer3: &[WeightedAndAdjustedEmissionRate3],
    im_merged: &[ImCoverageMerged],
) -> Vec<CriteriaRunningEmissionRow> {
    // WeightedAndAdjustedEmissionRate3 indexed for the six-column SHO3 join.
    let mut waer3_by_key: HashMap<[i32; 6], Vec<&WeightedAndAdjustedEmissionRate3>> =
        HashMap::new();
    for w in waer3 {
        waer3_by_key
            .entry([
                w.year_id,
                w.month_id,
                w.day_id,
                w.hour_id,
                w.source_type_id,
                w.model_year_id,
            ])
            .or_default()
            .push(w);
    }
    // Link keyed by its `linkID` primary key.
    let link_by_id: HashMap<i32, &LinkRow> = inputs.link.iter().map(|l| (l.link_id, l)).collect();
    // IMCoverageMergedUngrouped lookup for the I/M update — one row per key
    // (it is a GROUP BY result).
    let im_by_key: HashMap<[i32; 5], f64> = im_merged
        .iter()
        .map(|im| {
            (
                [
                    im.process_id,
                    im.pollutant_id,
                    im.model_year_id,
                    im.fuel_type_id,
                    im.source_type_id,
                ],
                im.im_adjust_fract,
            )
        })
        .collect();

    let mut out: Vec<CriteriaRunningEmissionRow> = Vec::new();
    for s in sho2 {
        // INNER JOIN WeightedAndAdjustedEmissionRate3 ON (yearID, monthID,
        // dayID, hourID, sourceTypeID, modelYearID).
        let Some(ws) = waer3_by_key.get(&[
            s.year_id,
            s.month_id,
            s.day_id,
            s.hour_id,
            s.source_type_id,
            s.model_year_id,
        ]) else {
            continue;
        };
        for w in ws {
            // INNER JOIN Link USING (linkID).
            let Some(l) = link_by_id.get(&w.link_id) else {
                continue;
            };
            let emission_quant = s.sho * w.mean_base_rate;
            let emission_quant_im = s.sho * w.mean_base_rate_im;
            // I/M update — blend on a match, GREATEST-clamp at 0.
            let final_quant = match im_by_key.get(&[
                w.process_id,
                w.pollutant_id,
                s.model_year_id,
                w.fuel_type_id,
                s.source_type_id,
            ]) {
                Some(&im_adjust_fract) => (emission_quant_im * im_adjust_fract
                    + emission_quant * (1.0 - im_adjust_fract))
                    .max(0.0),
                None => emission_quant,
            };
            out.push(CriteriaRunningEmissionRow {
                state_id: ctx.state_id,
                county_id: ctx.county_id,
                zone_id: l.zone_id,
                link_id: l.link_id,
                road_type_id: l.road_type_id,
                year_id: s.year_id,
                month_id: s.month_id,
                day_id: s.day_id,
                hour_id: s.hour_id,
                pollutant_id: w.pollutant_id,
                process_id: w.process_id,
                source_type_id: s.source_type_id,
                model_year_id: s.model_year_id,
                fuel_type_id: w.fuel_type_id,
                emission_quant: final_quant,
            });
        }
    }
    out
}

/// The MOVES criteria-pollutant running-exhaust calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input flows
/// through the [`CriteriaRunningInputs`] / [`RunContext`] arguments to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct CriteriaRunningCalculator {
    /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl CriteriaRunningCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator with its master-loop subscription.
    ///
    /// `CriteriaRunningCalculator` is a `GenericCalculatorBase` whose
    /// constructor passes the `polProcessID`s `101`/`201`/`301` (THC, CO and
    /// NOx Running Exhaust) at `MONTH` granularity with a `0` priority offset.
    /// `GenericCalculatorBase.subscribeToMe` issues one `targetLoop.subscribe`
    /// per process — and all three pairs share the Running Exhaust process —
    /// so the calculator carries a single subscription: Running Exhaust at
    /// `MONTH` granularity, `EMISSION_CALCULATOR` priority. `calculator-dag.json`
    /// records the same lone subscription with an unresolved process id
    /// (`process_id: 0`); this port resolves it to Running Exhaust.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                ProcessId(RUNNING_EXHAUST_PROCESS_ID),
                Granularity::Month,
                priority,
            )],
        }
    }

    /// Compute the criteria running-exhaust emission rows — the port of the
    /// `CriteriaRunningCalculator.sql` "Processing" section.
    ///
    /// The CREC steps run in order: CREC 1-a merges the I/M coverage, CREC 2
    /// builds the fuel-supply adjustment, CREC 3 the temperature adjustment,
    /// CREC 4 the air-conditioning adjustment, CREC-5 weights the emission
    /// rates by source bin, CREC-6 joins the operating-mode distribution,
    /// CREC 7 applies the fuel and temperature/AC adjustments and sums over
    /// operating mode, CREC 8 is the disabled-humidity pass-through, and
    /// CREC 9 multiplies by SHO activity and applies the I/M blend. The result
    /// is sorted by its integer dimension columns for deterministic output —
    /// MOVES leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(
        &self,
        inputs: &CriteriaRunningInputs,
        ctx: &RunContext,
    ) -> Vec<CriteriaRunningEmissionRow> {
        let im_merged = im_coverage_merged(inputs, ctx);
        let county_fuel = county_fuel_adjustment(inputs);
        let fuel_supply_ft = fuel_supply_with_fuel_type(inputs, ctx);
        let fuel_supply_adj = fuel_supply_adjustment(&county_fuel, &fuel_supply_ft);
        let met_adj = met_adjustment(inputs);
        let ac_on = ac_on_fraction(inputs);
        let ac_activity = ac_activity_fraction(inputs, ctx, &ac_on);
        let weighted_full_ac = weighted_full_ac_adjustment(inputs, ctx);
        let ac_adj = ac_adjustment(inputs, &ac_activity, &weighted_full_ac);
        let sb_weighted = sb_weighted_emission_rate(inputs, ctx);
        let fully_weighted = fully_weighted_emission_rate(inputs, ctx, &sb_weighted);
        let temp_and_ac = temp_and_ac_adjustment(&met_adj, &ac_adj);
        let fuel_adjusted = fuel_adjusted_rate(inputs, &fully_weighted, &fuel_supply_adj);
        let weighted_adjusted =
            weighted_and_adjusted_emission_rate(inputs, &fuel_adjusted, &temp_and_ac);
        let weighted_adjusted_2 = weighted_and_adjusted_emission_rate_2(inputs, &weighted_adjusted);
        let sho2 = build_sho2(inputs);
        let waer3 = weighted_and_adjusted_emission_rate_3(inputs, &weighted_adjusted_2);
        let mut output = assemble_emission_output(inputs, ctx, &sho2, &waer3, &im_merged);

        output.sort_unstable_by_key(CriteriaRunningEmissionRow::dimension_key);
        output
    }
}

impl Default for CriteriaRunningCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// `CriteriaRunningCalculator` registers no `(pollutant, process)` pairs — see
/// [`Calculator::registrations`] on the impl below.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB / execution-DB tables the criteria running computation consumes —
/// the data tables the SQL's "Extract Data" section pulls and the "Processing"
/// section reads. The SQL also extracts `Zone` and `PollutantProcessModelYear`;
/// neither feeds "Processing", so neither is listed.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "County",
    "EmissionRateByAge",
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "FuelType",
    "FullACAdjustment",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "Link",
    "ModelYear",
    "MonthGroupHour",
    "MonthOfAnyYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "SHO",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeAge",
    "SourceTypeModelYear",
    "TemperatureAdjustment",
    "Year",
    "ZoneMonthHour",
    "criteriaRatio",
];

impl Calculator for CriteriaRunningCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    /// `CriteriaRunningCalculator` registers **no** `(pollutant, process)`
    /// pairs.
    ///
    /// The Java `GenericCalculatorBase.doRegistration` registers THC, CO and
    /// NOx Running Exhaust — but those are legacy registrations. In the pinned
    /// MOVES, `CalculatorInfo.txt` (the runtime registration file) has no
    /// `Registration` directive for this module: the base-rate approach
    /// (`BaseRateCalculator`, Task 45) carries the criteria running-exhaust
    /// pairs, and `calculator-dag.json` records `registrations_count: 0` to
    /// match.
    ///
    /// Returning an empty slice keeps this port consistent with the runtime
    /// and prevents the registry from double-registering against
    /// `BaseRateCalculator`. See the [module docs](self).
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): `calculator-dag.json` records
    // `depends_on: []` and `subscribes_directly: true`. `CriteriaRunning
    // Calculator` subscribes directly to the master loop.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Phase 2 skeleton — returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the input tables or accept the
    /// worker-output rows — its row storage lands with the Task 50
    /// `DataFrameStore`. The computation itself is ported and tested in
    /// [`CriteriaRunningCalculator::calculate`]; see the
    /// [module documentation](self).
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let run_ctx = RunContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
        };
        let inputs = CriteriaRunningInputs {
            age_category: tables.iter_typed("AgeCategory")?,
            county: tables.iter_typed("County")?,
            criteria_ratio: tables.iter_typed("criteriaRatio")?,
            emission_rate_by_age: tables.iter_typed("EmissionRateByAge")?,
            fuel_formulation: tables.iter_typed("FuelFormulation")?,
            fuel_subtype: tables.iter_typed("FuelSubtype")?,
            fuel_supply: tables.iter_typed("FuelSupply")?,
            full_ac_adjustment: tables.iter_typed("FullACAdjustment")?,
            fuel_type: tables.iter_typed("FuelType")?,
            hour_day: tables.iter_typed("HourDay")?,
            im_coverage: tables.iter_typed("IMCoverage")?,
            im_factor: tables.iter_typed("IMFactor")?,
            link: tables.iter_typed("Link")?,
            model_year: tables.iter_typed("ModelYear")?,
            month_group_hour: tables.iter_typed("MonthGroupHour")?,
            month_of_any_year: tables.iter_typed("MonthOfAnyYear")?,
            op_mode_distribution: tables.iter_typed("OpModeDistribution")?,
            pollutant_process_assoc: tables.iter_typed("PollutantProcessAssoc")?,
            pollutant_process_mapped_model_year: tables.iter_typed("PollutantProcessMappedModelYear")?,
            sho: tables.iter_typed("SHO")?,
            source_bin: tables.iter_typed("SourceBin")?,
            source_bin_distribution: tables.iter_typed("SourceBinDistribution")?,
            source_type_age: tables.iter_typed("SourceTypeAge")?,
            source_type_model_year: tables.iter_typed("SourceTypeModelYear")?,
            temperature_adjustment: tables.iter_typed("TemperatureAdjustment")?,
            year: tables.iter_typed("Year")?,
            zone_month_hour: tables.iter_typed("ZoneMonthHour")?,
        };
        let rows = self.calculate(&inputs, &run_ctx);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(CriteriaRunningCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CO Running Exhaust `polProcessID` — `pollutantID 2 × 100 + processID 1`.
    const CO_RUNNING_POL_PROCESS: i32 = 201;

    /// The run context the test fixtures use: calendar year 2020, county
    /// 26161, zone 90, link 5001, state 26.
    fn run_context() -> RunContext {
        RunContext {
            year: 2020,
            county_id: 26_161,
            zone_id: 90,
            link_id: 5001,
            state_id: 26,
        }
    }

    /// A minimal one-of-everything input that threads exactly one row through
    /// all CREC steps, parameterised by `polProcessID` / `pollutantID` so the
    /// NOx-humidity test can exercise the CREC 8 `TEMP1` branch.
    ///
    /// Hand-computed with `temperature = 75` (zero temperature delta → factor
    /// `1.0`), `heatIndex = 0` and `fullACAdjustment = 1.0` (AC factor `1.0`):
    /// CREC 1-a `IMAdjustFract = 1.0 × 50.0 × 0.01 = 0.5`; CREC 2 `fuelAdjustment
    /// = 3.0 + 0.0 × (9.0 − 3.0) = 3.0`, market-share-weighted to `3.0 × 1.0 =
    /// 3.0`; CREC-5 carries `meanBaseRate = 1.0 × 10.0 = 10.0`, `meanBaseRateIM
    /// = 1.0 × 4.0 = 4.0`; CREC-6 carries the single op mode; CREC 7-b
    /// `fuelAdjustedRate = 10.0 × 3.0 = 30.0`, `…IM = 4.0 × 3.0 = 12.0`; CREC
    /// 7-c `meanBaseRate = 30.0 × 1.0 × 1.0 = 30.0`, `…IM = 12.0`; CREC 9
    /// `emissionQuant = 30.0 × 100.0 = 3000.0`, `…IM = 12.0 × 100.0 = 1200.0`;
    /// I/M blend `max(1200.0 × 0.5 + 3000.0 × 0.5, 0) = 2100.0`.
    fn inputs_for(pol_process_id: i32, pollutant_id: i32) -> CriteriaRunningInputs {
        CriteriaRunningInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                age_group_id: 300,
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                gpa_fract: 0.0,
            }],
            criteria_ratio: vec![CriteriaRatioRow {
                pol_process_id,
                fuel_formulation_id: 100,
                source_type_id: 21,
                model_year_id: 2018,
                ratio: 3.0,
                ratio_gpa: 9.0,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 5000,
                pol_process_id,
                op_mode_id: 100,
                age_group_id: 300,
                mean_base_rate: 10.0,
                mean_base_rate_im: 4.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
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
            full_ac_adjustment: vec![FullAcAdjustmentRow {
                source_type_id: 21,
                pol_process_id,
                op_mode_id: 100,
                full_ac_adjustment: 1.0,
            }],
            fuel_type: vec![FuelTypeRow { fuel_type_id: 1 }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            im_coverage: vec![ImCoverageRow {
                pol_process_id,
                source_type_id: 21,
                fuel_type_id: 1,
                beg_model_year_id: 2010,
                end_model_year_id: 2020,
                inspect_freq: 1,
                test_standards_id: 1,
                compliance_factor: 50.0,
            }],
            im_factor: vec![ImFactorRow {
                pol_process_id,
                inspect_freq: 1,
                test_standards_id: 1,
                source_type_id: 21,
                fuel_type_id: 1,
                im_model_year_group_id: 500,
                age_group_id: 300,
                im_factor: 1.0,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                zone_id: 90,
                road_type_id: 4,
            }],
            model_year: vec![ModelYearRow {
                model_year_id: 2018,
            }],
            month_group_hour: vec![MonthGroupHourRow {
                month_group_id: 7,
                hour_id: 8,
                ac_activity_term_a: 1.0,
                ac_activity_term_b: 0.0,
                ac_activity_term_c: 0.0,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 7,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 85,
                pol_process_id,
                op_mode_id: 100,
                op_mode_fraction: 1.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id,
                pollutant_id,
                process_id: 1,
            }],
            pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
                pol_process_id,
                model_year_id: 2018,
                im_model_year_group_id: 500,
            }],
            sho: vec![ShoRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2,
                source_type_id: 21,
                sho: 100.0,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 5000,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id,
                source_bin_id: 5000,
                source_bin_activity_fraction: 1.0,
            }],
            source_type_age: vec![SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 2,
                functioning_ac_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                source_type_id: 21,
                model_year_id: 2018,
                ac_penetration_fraction: 1.0,
            }],
            temperature_adjustment: vec![TemperatureAdjustmentRow {
                pol_process_id,
                fuel_type_id: 1,
                min_model_year_id: 1990,
                max_model_year_id: 2050,
                temp_adjust_term_a: 0.02,
                temp_adjust_term_b: 0.0004,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            zone_month_hour: vec![ZoneMonthHourRow {
                zone_id: 90,
                month_id: 7,
                hour_id: 8,
                temperature: 75.0, // at the reference temperature → factor 1.0
                heat_index: 0.0,
            }],
        }
    }

    /// CO Running Exhaust one-of-everything fixture.
    fn minimal_inputs() -> CriteriaRunningInputs {
        inputs_for(CO_RUNNING_POL_PROCESS, 2)
    }

    /// Assert `actual` matches `expected` within `f64` slack — the
    /// FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_quant(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "emission_quant {actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let calc = CriteriaRunningCalculator::new();
        let rows = calc.calculate(&minimal_inputs(), &run_context());
        assert_eq!(rows.len(), 1);
        let row = rows[0];
        assert_quant(row.emission_quant, 2100.0);
        assert_eq!(row.state_id, 26);
        assert_eq!(row.county_id, 26_161);
        assert_eq!(row.zone_id, 90);
        assert_eq!(row.link_id, 5001);
        assert_eq!(row.road_type_id, 4);
        assert_eq!(row.year_id, 2020);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 8);
        assert_eq!(row.pollutant_id, 2);
        assert_eq!(row.process_id, 1);
        assert_eq!(row.source_type_id, 21);
        assert_eq!(row.model_year_id, 2018);
        assert_eq!(row.fuel_type_id, 1);
    }

    #[test]
    fn calculate_applies_the_temperature_adjustment() {
        // temperature 50 → d = 50 - 75 = -25; factor =
        // 1 + d × (0.02 + d × 0.0004) = 1 + (-25) × (0.02 - 0.01)
        //   = 1 + (-25) × 0.01 = 0.75.
        // CREC 7-c meanBaseRate = 30 × 0.75 = 22.5, …IM = 12 × 0.75 = 9.
        // CREC 9 ×100 → 2250 / 900; I/M blend max(900×0.5 + 2250×0.5, 0) = 1575.
        let mut inputs = minimal_inputs();
        inputs.zone_month_hour[0].temperature = 50.0;
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 1575.0);
    }

    #[test]
    fn calculate_applies_the_air_conditioning_adjustment() {
        // fullACAdjustment 3.0 → weightedFullACAdjustment 3.0; ACActivityFraction
        // = ACOnFraction(1.0) × ACPenetration(1.0) × functioningAC(1.0) = 1.0;
        // ACAdjustment = 1 + (3.0 - 1) × 1.0 = 3.0. tempAndACAdjustment =
        // 1.0 × 3.0 = 3.0. CREC 7-c meanBaseRate = 30 × 3.0 = 90, …IM = 36.
        // CREC 9 ×100 → 9000 / 3600; I/M blend max(3600×0.5 + 9000×0.5, 0) = 6300.
        let mut inputs = minimal_inputs();
        inputs.full_ac_adjustment[0].full_ac_adjustment = 3.0;
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 6300.0);
    }

    #[test]
    fn calculate_without_im_coverage_leaves_emission_unadjusted() {
        // No IMFactor / IMCoverage → no IMCoverageMergedUngrouped row → the I/M
        // update finds no match, so emissionQuant = meanBaseRate × SHO =
        // 30 × 100 = 3000 (no blend with the I/M rate).
        let mut inputs = minimal_inputs();
        inputs.im_factor.clear();
        inputs.im_coverage.clear();
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 3000.0);
    }

    #[test]
    fn calculate_clamps_negative_im_blend_to_zero() {
        // Negative base rates drive both emissionQuant and emissionQuantIM
        // negative; GREATEST(..., 0.0) clamps the I/M blend to zero.
        let mut inputs = minimal_inputs();
        inputs.emission_rate_by_age[0].mean_base_rate = -10.0;
        inputs.emission_rate_by_age[0].mean_base_rate_im = -4.0;
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 0.0);
    }

    #[test]
    fn calculate_weights_emission_rates_across_source_bins() {
        // Two source bins, both fuel type 1 / op mode 100, with activity
        // fractions 0.6 and 0.4. CREC-5 sums: meanBaseRate =
        // 10 × 0.6 + 20 × 0.4 = 14. CREC 7-b ×3 → 42; CREC 9 ×100 → 4200.
        // No I/M coverage, so emission = 4200.
        let mut inputs = minimal_inputs();
        inputs.im_factor.clear();
        inputs.im_coverage.clear();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 5001,
            fuel_type_id: 1,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 5001,
            pol_process_id: CO_RUNNING_POL_PROCESS,
            op_mode_id: 100,
            age_group_id: 300,
            mean_base_rate: 20.0,
            mean_base_rate_im: 20.0,
        });
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.6;
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: CO_RUNNING_POL_PROCESS,
                source_bin_id: 5001,
                source_bin_activity_fraction: 0.4,
            });
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 4200.0);
    }

    #[test]
    fn calculate_sums_emission_rates_across_operating_modes() {
        // Two operating modes, 100 (rate 10) and 200 (rate 20), with op-mode
        // fractions 0.7 and 0.3. CREC-5 keeps a row per op mode; CREC 7-c sums
        // over op mode: meanBaseRate = (10×3)×1×0.7 + (20×3)×1×0.3 = 21 + 18 =
        // 39. CREC 9 ×100 → 3900. No I/M coverage → emission = 3900.
        let mut inputs = minimal_inputs();
        inputs.im_factor.clear();
        inputs.im_coverage.clear();
        inputs.op_mode_distribution[0].op_mode_fraction = 0.7;
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            source_type_id: 21,
            hour_day_id: 85,
            pol_process_id: CO_RUNNING_POL_PROCESS,
            op_mode_id: 200,
            op_mode_fraction: 0.3,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 5000,
            pol_process_id: CO_RUNNING_POL_PROCESS,
            op_mode_id: 200,
            age_group_id: 300,
            mean_base_rate: 20.0,
            mean_base_rate_im: 20.0,
        });
        inputs.full_ac_adjustment.push(FullAcAdjustmentRow {
            source_type_id: 21,
            pol_process_id: CO_RUNNING_POL_PROCESS,
            op_mode_id: 200,
            full_ac_adjustment: 1.0,
        });
        let rows = CriteriaRunningCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 3900.0);
    }

    #[test]
    fn calculate_nox_humidity_branch_is_a_passthrough() {
        // NOx Running Exhaust (polProcessID 301) routes through CREC 8's
        // WeightedAndAdjustedEmissionRate2_TEMP1 — inner-joined to Link,
        // ZoneMonthHour and FuelType. The humidity correction is disabled, so
        // the result is the same 2100.0 the CO (TEMP2) fixture produces.
        let rows = CriteriaRunningCalculator::new().calculate(&inputs_for(301, 3), &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 2100.0);
        assert_eq!(rows[0].pollutant_id, 3);
        assert_eq!(rows[0].process_id, 1);
    }

    #[test]
    fn calculate_empty_inputs_yields_no_rows() {
        let calc = CriteriaRunningCalculator::new();
        let rows = calc.calculate(&CriteriaRunningInputs::default(), &run_context());
        assert!(rows.is_empty());
    }

    #[test]
    fn calculator_metadata_matches_the_runtime() {
        let calc = CriteriaRunningCalculator::new();
        assert_eq!(calc.name(), "CriteriaRunningCalculator");

        // One subscription: Running Exhaust, MONTH, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(1));
        assert_eq!(subs[0].granularity, Granularity::Month);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

        // Superseded by BaseRateCalculator — no registrations.
        assert!(calc.registrations().is_empty());
        // Subscribes directly — no upstream chain dependency.
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"TemperatureAdjustment"));
        assert!(calc.input_tables().contains(&"SHO"));
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CriteriaRunningCalculator");
        let calcs: Vec<Box<dyn Calculator>> = vec![factory()];
        assert_eq!(calcs[0].name(), "CriteriaRunningCalculator");
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let inputs = inputs_for(CO_RUNNING_POL_PROCESS, 2);
        let mut store = InMemoryStore::new();
        store.insert("AgeCategory", AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap());
        store.insert("County", CountyRow::into_dataframe(inputs.county.clone()).unwrap());
        store.insert("criteriaRatio", CriteriaRatioRow::into_dataframe(inputs.criteria_ratio.clone()).unwrap());
        store.insert("EmissionRateByAge", EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age.clone()).unwrap());
        store.insert("FuelFormulation", FuelFormulationRow::into_dataframe(inputs.fuel_formulation.clone()).unwrap());
        store.insert("FuelSubtype", FuelSubtypeRow::into_dataframe(inputs.fuel_subtype.clone()).unwrap());
        store.insert("FuelSupply", FuelSupplyRow::into_dataframe(inputs.fuel_supply.clone()).unwrap());
        store.insert("FullACAdjustment", FullAcAdjustmentRow::into_dataframe(inputs.full_ac_adjustment.clone()).unwrap());
        store.insert("FuelType", FuelTypeRow::into_dataframe(inputs.fuel_type.clone()).unwrap());
        store.insert("HourDay", HourDayRow::into_dataframe(inputs.hour_day.clone()).unwrap());
        store.insert("IMCoverage", ImCoverageRow::into_dataframe(inputs.im_coverage.clone()).unwrap());
        store.insert("IMFactor", ImFactorRow::into_dataframe(inputs.im_factor.clone()).unwrap());
        store.insert("Link", LinkRow::into_dataframe(inputs.link.clone()).unwrap());
        store.insert("ModelYear", ModelYearRow::into_dataframe(inputs.model_year.clone()).unwrap());
        store.insert("MonthGroupHour", MonthGroupHourRow::into_dataframe(inputs.month_group_hour.clone()).unwrap());
        store.insert("MonthOfAnyYear", MonthOfAnyYearRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap());
        store.insert("OpModeDistribution", OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution.clone()).unwrap());
        store.insert("PollutantProcessAssoc", PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc.clone()).unwrap());
        store.insert("PollutantProcessMappedModelYear", PollutantProcessMappedModelYearRow::into_dataframe(inputs.pollutant_process_mapped_model_year.clone()).unwrap());
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho.clone()).unwrap());
        store.insert("SourceBin", SourceBinRow::into_dataframe(inputs.source_bin.clone()).unwrap());
        store.insert("SourceBinDistribution", SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution.clone()).unwrap());
        store.insert("SourceTypeAge", SourceTypeAgeRow::into_dataframe(inputs.source_type_age.clone()).unwrap());
        store.insert("SourceTypeModelYear", SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap());
        store.insert("TemperatureAdjustment", TemperatureAdjustmentRow::into_dataframe(inputs.temperature_adjustment.clone()).unwrap());
        store.insert("Year", YearRow::into_dataframe(inputs.year.clone()).unwrap());
        store.insert("ZoneMonthHour", ZoneMonthHourRow::into_dataframe(inputs.zone_month_hour.clone()).unwrap());
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = CriteriaRunningCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(out.dataframe().unwrap().height() > 0, "expected at least one row");
    }
}
