//! Port of `TankVaporVentingCalculator.java` and
//! `database/TankVaporVentingCalculator.sql` — migration plan Phase 3,
//! Task 59.
//!
//! `TankVaporVentingCalculator` produces the **tank vapor venting** emission
//! of total gaseous hydrocarbons — the THC (pollutant 1) vented from a
//! vehicle's fuel tank as the fuel warms and cools through hot-soak and
//! diurnal cycles. It registers exactly one `(pollutant, process)` pair:
//! THC × Evap Fuel Vapor Venting (process 12), and subscribes to that
//! process at `MONTH` granularity.
//!
//! # What it computes
//!
//! Vapor venting is driven by the rise and fall of fuel-tank temperature
//! during a cold soak. For each hour up to the day's peak cold-soak
//! temperature the calculator computes how much vapor the tank *generates*
//! (`TVG`), converts that to cumulative vapor *vented* past the canister
//! (`TVV`) via per-regulatory-class polynomial coefficients, differences it
//! into an hourly rate, weights it back onto source bins, blends in the
//! I/M-program adjustment, and multiplies by source operating hours:
//!
//! ```text
//! emissionQuant = weightedMeanBaseRate × sourceHours × opModeFraction
//! ```
//!
//! with the I/M-adjusted value `emissionQuantIM × IMAdjustFract +
//! emissionQuant × (1 − IMAdjustFract)` substituted where an I/M program
//! covers the `(process, pollutant, modelYear, fuelType, sourceType)`.
//!
//! # Algorithm
//!
//! [`TankVaporVentingCalculator::calculate`] ports the SQL's "Processing"
//! section. The SQL builds nine MyISAM working tables across nine numbered
//! steps; the port keeps each step as a function and threads the working
//! tables through as plain row vectors:
//!
//! | SQL step | SQL working table | This port |
//! |----------|-------------------|-----------|
//! | TVV-1 | `IMCoverageMergedUngrouped` | `im_coverage_merged_ungrouped` |
//! | TVV-2 | `PeakHourOfColdSoak` | `peak_hour_of_cold_soak` |
//! | TVV-3 | `TankVaporGenerated` | `tank_vapor_generated` |
//! | TVV-4 | `EthanolWeightedTVG` | `ethanol_weighted_tvg` |
//! | TVV-5 | `CummulativeTankVaporVented` | `cumulative_tank_vapor_vented` |
//! | TVV-6 | `UnweightedHourlyTVV` | `unweighted_hourly_tvv` |
//! | TVV-7 | `HourlyTVV` | `hourly_tvv` |
//! | TVV-8 | `WeightedMeanBaseRate` | `weighted_mean_base_rate` |
//! | TVV-9 | `MOVESWorkerOutput` | `assemble_emission_output` |
//!
//! Every join in the SQL is an `INNER JOIN`, so a row with no match on the
//! join key is dropped; the port reproduces that with map lookups that skip
//! on a miss. The exceptions are TVV-6's `LEFT JOIN` of
//! `CummulativeTankVaporVented` onto its own prior hour (a miss contributes
//! `coalesce(…, 0)`), and three `ON`-less joins that are cartesian products
//! — `CumTVVCoeffs × EthanolWeightedTVG` in TVV-5 and `… × RunSpecMonth ×
//! RunSpecHourDay` in TVV-8 — which the port writes as nested loops.
//!
//! # Single-day vs. multiday
//!
//! `TankVaporVentingCalculator.java` selects one of two SQL scripts by the
//! `USE_MULTIDAY_DIURNALS` compilation flag: `TankVaporVentingCalculator.sql`
//! (single-day, ported here) or `MultidayTankVaporVentingCalculator.sql`
//! (Task 60). The pinned MOVES build compiles with the flag set, so the
//! single-day script carries an `@notused` annotation — but the *calculator*
//! is live (`CalculatorInfo.txt` records its `Registration` and `Subscribe`
//! directives regardless of which script runs). Migration-plan Task 59
//! ports the single-day script; the Java `alterReplacementsAndSections`
//! equation machinery (`##tvvEquations##` / `##leakEquations##`,
//! `sampleVehicleSoaking*`) is multiday-only and is not part of this port.
//!
//! # Scope of this port
//!
//! [`calculate`](TankVaporVentingCalculator::calculate) is the SQL
//! "Processing" section. The SQL's "Extract Data" section — the `cache
//! SELECT … INTO OUTFILE` statements that filter the default and execution
//! databases by run context — is data-plane wiring, not algorithm: a
//! [`TankVaporVentingInputs`] *is* the post-extract tables, so the port does
//! not re-apply the extract `WHERE` clauses (`zoneID`, `monthID`, `yearID`,
//! `countyID`, `linkID`, `polProcessID`, `opModeID`, and model-year-range
//! filters). Where the Processing section repeats an extract filter — every
//! `polProcessID IN (##pollutantProcessIDs##)`, the `opModeID IN (150,300)`
//! on `EmissionRateByAge`, and `IMCoverage.countyID` / `IMCoverage.yearID` —
//! the port omits it as redundant given correctly-extracted inputs.
//! Processing-section joins, `ON` clauses, `CASE`s, `GROUP BY`s and
//! arithmetic are ported faithfully.
//!
//! # Fidelity notes
//!
//! `TankVaporVentingCalculator.sql` stores every working-table measure in a
//! `FLOAT` (32-bit) column while MariaDB evaluates the arithmetic in
//! `DOUBLE`. This port sums, multiplies and exponentiates in `f64` end to
//! end, so it does not reproduce the `f32` truncation MOVES applies between
//! steps — a sub-`1e-7` relative drift. Reproducing it bug-for-bug is the
//! calculator-integration-validation call (Task `mo-fvuf`, which this task
//! blocks), matching the Task 41 / Task 33 / Task 58 precedent. Several
//! extracted `FLOAT` columns (`RVP`, `ETOHVolume`, the `tvgTerm*` /
//! `tvvTerm*` coefficients, `meanBaseRate*`, `complianceFactor`) are
//! schema-nullable but are model coefficient / input data populated for
//! every row the calculator processes; following the Task 58 precedent they
//! are modelled as `f64`. There are no integer/integer literal divisions in
//! the SQL, so the MariaDB `div_precision_increment` rounding gotcha does
//! not arise. TVV-2's peak-hour pack/unpack idiom — `round(T,2)*100000 +
//! (999−hourID)`, then `mod(…,1000)` — is computed in exact integer
//! arithmetic here (see `peak_hour_of_cold_soak`), reproducing its intent
//! (peak = highest 2-decimal-rounded temperature, ties to earliest hour)
//! without the float noise the SQL's `mod` is exposed to.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm
//! is fully ported and unit-tested on
//! [`calculate`](TankVaporVentingCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises a [`TankVaporVentingInputs`] and a
//! [`RunContext`] from `ctx.tables()` / `ctx.position()`, calls
//! [`calculate`](TankVaporVentingCalculator::calculate), and writes the rows
//! to the worker output.

use std::collections::{HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription,
    DataFrameStoreTyped, Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the
/// `TankVaporVentingCalculator` entry in the calculator-chain DAG
/// (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "TankVaporVentingCalculator";

/// Evap Fuel Vapor Venting — `EmissionProcess` row 12. The calculator's only
/// process: it subscribes to it and registers one pollutant for it.
const EVAP_FUEL_VAPOR_VENTING_PROCESS_ID: u16 = 12;

/// Total Gaseous Hydrocarbons — `Pollutant` row 1. The single pollutant the
/// calculator registers.
const TOTAL_HYDROCARBONS_POLLUTANT_ID: u16 = 1;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `TankVaporVenting
// Calculator.sql`'s "Extract Data" section pulls. Following the Phase 3
// convention every `INT`/`SMALLINT` identifier is an `i32`, `sourceBinID`
// (`BIGINT`) is an `i64`, and every `FLOAT`/`DOUBLE` quantity is an `f64`.
// Only the columns the venting algorithm reads are modelled.
// ===========================================================================

/// One `AgeCategory` row — the age-group bucket for a vehicle age.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageID` — vehicle age in years; the unique primary key.
    pub age_id: i32,
    /// `ageGroupID` — the age-group bucket the age falls in.
    pub age_group_id: i32,
}

/// One `AverageTankGasoline` row — a fuel blend's tank-gasoline properties
/// for a month group and fuel year.
///
/// `AverageTankGasoline` is extracted to the run's single zone, so the
/// venting algorithm joins it on `(monthGroupID, fuelYearID, fuelTypeID)`
/// only and `zoneID` is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageTankGasolineRow {
    /// `fuelTypeID` — the fuel type.
    pub fuel_type_id: i32,
    /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
    /// `monthGroupID` — the month group.
    pub month_group_id: i32,
    /// `ETOHVolume` — ethanol volume percent. `FLOAT` (schema-nullable);
    /// modelled as `f64` (see the module fidelity notes).
    pub etoh_volume: f64,
    /// `RVP` — Reid vapor pressure of the tank gasoline. `FLOAT`
    /// (schema-nullable); modelled as `f64`.
    pub rvp: f64,
}

/// One `ColdSoakInitialHourFraction` row — the fraction of a source type's
/// activity that began its cold soak in a given initial hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdSoakInitialHourFractionRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourDayID` — the current hour-day.
    pub hour_day_id: i32,
    /// `initialHourDayID` — the hour-day the cold soak began.
    pub initial_hour_day_id: i32,
    /// `coldSoakInitialHourFraction` — the activity fraction.
    pub cold_soak_initial_hour_fraction: f64,
}

/// One `ColdSoakTankTemperature` row — a zone/month/hour cold-soak fuel-tank
/// temperature.
///
/// `ColdSoakTankTemperature` is extracted to the run's single zone, so the
/// venting algorithm joins it on `(monthID, hourID)` only and `zoneID` is
/// not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdSoakTankTemperatureRow {
    /// `monthID`.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `coldSoakTankTemperature` — the cold-soak tank temperature (°F).
    pub cold_soak_tank_temperature: f64,
}

/// One `County` row — supplies the altitude bucket of a county.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID` — the county primary key.
    pub county_id: i32,
    /// `altitude` — the `CHAR(1)` altitude bucket (`'L'` low / `'H'` high);
    /// joins to [`TankVaporGenCoeffsRow::altitude`]. Schema-nullable;
    /// modelled as a populated `char`.
    pub altitude: char,
}

/// One `CumTVVCoeffs` row — the cumulative tank-vapor-vented polynomial
/// coefficients for a regulatory class, model-year group and age group.
///
/// The single-day script reads only the six base / I/M polynomial terms;
/// the `*CV` uncertainty terms, the multiday `tvvEquation` / `leakEquation`
/// strings and the canister / leak columns are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CumTvvCoeffsRow {
    /// `regClassID` — regulatory class.
    pub reg_class_id: i32,
    /// `modelYearGroupID` — model-year group.
    pub model_year_group_id: i32,
    /// `ageGroupID` — age group.
    pub age_group_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `tvvTermA` — the constant term of the base TVV polynomial.
    pub tvv_term_a: f64,
    /// `tvvTermB` — the linear term of the base TVV polynomial.
    pub tvv_term_b: f64,
    /// `tvvTermC` — the quadratic term of the base TVV polynomial.
    pub tvv_term_c: f64,
    /// `tvvTermAIM` — the constant term of the I/M TVV polynomial.
    pub tvv_term_a_im: f64,
    /// `tvvTermBIM` — the linear term of the I/M TVV polynomial.
    pub tvv_term_b_im: f64,
    /// `tvvTermCIM` — the quadratic term of the I/M TVV polynomial.
    pub tvv_term_c_im: f64,
}

/// One `EmissionRateByAge` row — a source bin's mean base rate for an
/// age group and operating mode.
///
/// `EmissionRateByAge` is extracted to operating modes 150 and 300; TVV-8's
/// `opModeID IN (150,300)` filter is that extract repeated and the port
/// omits it (see the module scope notes). `opModeID` is carried because
/// TVV-8 writes it onto `WeightedMeanBaseRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `sourceBinID` — `BIGINT`; joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — the operating mode (150 hot soak / 300 operating).
    pub op_mode_id: i32,
    /// `ageGroupID` — the age-group bucket.
    pub age_group_id: i32,
    /// `meanBaseRate` — the mean emission base rate.
    pub mean_base_rate: f64,
    /// `meanBaseRateIM` — the mean emission base rate under I/M.
    pub mean_base_rate_im: f64,
}

/// One `FuelType` row — supplies the evap-calculations eligibility flag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelTypeRow {
    /// `fuelTypeID` — the fuel type.
    pub fuel_type_id: i32,
    /// `subjectToEvapCalculations` — the `CHAR(1)` `'Y'`/`'N'` flag,
    /// modelled as a `bool`; TVV-8 keeps only `'Y'` fuel types.
    pub subject_to_evap_calculations: bool,
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

/// One `IMCoverage` row — an I/M program's coverage of a
/// `(sourceType, fuelType)` and model-year range.
///
/// `IMCoverage` is extracted to the run's county and year with
/// `useIMyn = 'Y'`; TVV-1's `countyID` / `yearID` filters are those extract
/// filters repeated and the port omits them. `inspectFreq` and
/// `testStandardsID` are schema-nullable join keys modelled as `i32`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImCoverageRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `inspectFreq` — inspection frequency; joins to
    /// [`ImFactorRow::inspect_freq`].
    pub inspect_freq: i32,
    /// `testStandardsID` — test-standards bucket; joins to
    /// [`ImFactorRow::test_standards_id`].
    pub test_standards_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `begModelYearID` — inclusive lower model-year bound of the coverage.
    pub beg_model_year_id: i32,
    /// `endModelYearID` — inclusive upper model-year bound of the coverage.
    pub end_model_year_id: i32,
    /// `complianceFactor` — the program's compliance factor (percent).
    pub compliance_factor: f64,
}

/// One `IMFactor` row — an I/M program's adjustment factor for a
/// `(polProcess, inspection, test, sourceType, fuelType, modelYearGroup,
/// ageGroup)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImFactorRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `inspectFreq` — inspection frequency.
    pub inspect_freq: i32,
    /// `testStandardsID` — test-standards bucket.
    pub test_standards_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `IMModelYearGroupID` — joins to
    /// [`PollutantProcessModelYearRow::im_model_year_group_id`].
    pub im_model_year_group_id: i32,
    /// `ageGroupID` — joins to [`AgeCategoryRow::age_group_id`].
    pub age_group_id: i32,
    /// `IMFactor` — the I/M adjustment factor.
    pub im_factor: f64,
}

/// One `MonthOfAnyYear` row — the `monthID` → `monthGroupID` map.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthOfAnyYearRow {
    /// `monthID`.
    pub month_id: i32,
    /// `monthGroupID` — the month group the month belongs to.
    pub month_group_id: i32,
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

/// One `PollutantProcessModelYear` row — maps a model year onto its
/// model-year group and I/M model-year group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — the model year.
    pub model_year_id: i32,
    /// `modelYearGroupID` — the model-year group.
    pub model_year_group_id: i32,
    /// `IMModelYearGroupID` — the I/M model-year group. Schema-nullable;
    /// modelled as `i32` and populated for I/M-relevant rows.
    pub im_model_year_group_id: i32,
}

/// One `SourceBin` row — supplies the fuel type, regulatory class and
/// model-year group of a source bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID` — `BIGINT` primary key.
    pub source_bin_id: i64,
    /// `fuelTypeID` — the source bin's fuel type.
    pub fuel_type_id: i32,
    /// `regClassID` — the source bin's regulatory class. Schema-nullable;
    /// modelled as `i32`.
    pub reg_class_id: i32,
    /// `modelYearGroupID` — the source bin's model-year group.
    /// Schema-nullable; modelled as `i32`.
    pub model_year_group_id: i32,
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
///
/// `SourceHours` is extracted to the run's month, year and link; TVV-9's
/// `yearID` / `linkID` join conditions are those extract filters repeated
/// and the port omits them, keeping the genuine `monthID`, `hourDayID`,
/// `ageID` and `sourceTypeID` joins.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
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

/// One `TankVaporGenCoeffs` row — the tank-vapor-generated coefficients for
/// an ethanol level and altitude bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TankVaporGenCoeffsRow {
    /// `ethanolLevelID` — the ethanol level (0 or 10) the coefficients
    /// apply to; TVV-4 interpolates between the two.
    pub ethanol_level_id: i32,
    /// `altitude` — the `CHAR(1)` altitude bucket; joins to
    /// [`CountyRow::altitude`].
    pub altitude: char,
    /// `tvgTermA` — the multiplicative term of the TVG equation.
    pub tvg_term_a: f64,
    /// `tvgTermB` — the RVP exponential coefficient of the TVG equation.
    pub tvg_term_b: f64,
    /// `tvgTermC` — the temperature exponential coefficient of the TVG
    /// equation.
    pub tvg_term_c: f64,
}

/// One `Year` row — resolves a calendar year into its fuel year.
///
/// `Year` is extracted to the run's single calendar year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
}

/// One `Zone` row — resolves a zone into its county.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRow {
    /// `zoneID` — the zone primary key.
    pub zone_id: i32,
    /// `countyID` — the county the zone belongs to.
    pub county_id: i32,
}

/// Inputs to [`TankVaporVentingCalculator::calculate`] — the extracted
/// tables the SQL's "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the
/// per-run filtered execution database; until then it is the explicit
/// data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct TankVaporVentingInputs {
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `AverageTankGasoline` rows.
    pub average_tank_gasoline: Vec<AverageTankGasolineRow>,
    /// `ColdSoakInitialHourFraction` rows.
    pub cold_soak_initial_hour_fraction: Vec<ColdSoakInitialHourFractionRow>,
    /// `ColdSoakTankTemperature` rows.
    pub cold_soak_tank_temperature: Vec<ColdSoakTankTemperatureRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// `CumTVVCoeffs` rows.
    pub cum_tvv_coeffs: Vec<CumTvvCoeffsRow>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `FuelType` rows.
    pub fuel_type: Vec<FuelTypeRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `IMCoverage` rows.
    pub im_coverage: Vec<ImCoverageRow>,
    /// `IMFactor` rows.
    pub im_factor: Vec<ImFactorRow>,
    /// `MonthOfAnyYear` rows.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `PollutantProcessModelYear` rows.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `RunSpecHourDay` — the `hourDayID`s the run processes; TVV-8's second
    /// insert cross-joins this.
    pub run_spec_hour_day: Vec<i32>,
    /// `RunSpecMonth` — the `monthID`s the run processes; TVV-8's second
    /// insert cross-joins this.
    pub run_spec_month: Vec<i32>,
    /// `RunSpecSourceType` — the `sourceTypeID`s the run processes; TVV-8's
    /// second insert joins this to keep run-spec source types only.
    pub run_spec_source_type: Vec<i32>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceHours` rows.
    pub source_hours: Vec<SourceHoursRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `TankVaporGenCoeffs` rows.
    pub tank_vapor_gen_coeffs: Vec<TankVaporGenCoeffsRow>,
    /// `Year` rows.
    pub year: Vec<YearRow>,
    /// `Zone` rows.
    pub zone: Vec<ZoneRow>,
}

/// The per-run scalar context [`TankVaporVentingCalculator::calculate`]
/// reads — the `##context.*##` substitutions the SQL preprocessor resolves
/// before running the script.
///
/// The TVV script has no `WithRegClassID` / `NoRegClassID` section toggle
/// and no source-type loop, so the context is purely the iteration-location
/// identifiers TVV-9 stamps onto the output plus the calendar year TVV-1,
/// TVV-5 and TVV-8 derive vehicle age from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunContext {
    /// `##context.year##` — the run's calendar year. Used to derive vehicle
    /// age (`year − modelYearID` / `year − ageID`) in TVV-1, TVV-5 and
    /// TVV-8, and stamped as `yearID` on the output in TVV-9.
    pub year: i32,
    /// `##context.iterLocation.stateRecordID##` — stamped as `stateID` in
    /// TVV-9.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##` — stamped as `countyID` in
    /// TVV-9.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##` — the `UnweightedHourlyTVV`
    /// `zoneID` default in TVV-6 and the `zoneID` stamped in TVV-9.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##` — stamped as `linkID` in
    /// TVV-9.
    pub link_id: i32,
    /// `##context.iterLocation.roadTypeRecordID##` — stamped as `roadTypeID`
    /// in TVV-9.
    pub road_type_id: i32,
}

/// One `MOVESWorkerOutput` row produced by the venting calculation — the
/// TVV-9 output, after the I/M-adjustment `UPDATE`.
///
/// `SCC` is written `NULL` by the SQL and is not an algorithm input; it is
/// left to the Task 50 output wiring and not modelled. `emissionQuant`
/// carries the I/M-adjusted emission.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TankVaporVentingEmissionRow {
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
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — the I/M-adjusted venting emission. Equal to
    /// `weightedMeanBaseRate × sourceHours × opModeFraction` where no I/M
    /// program applies, else the I/M blend (see TVV-9).
    pub emission_quant: f64,
}

impl TankVaporVentingEmissionRow {
    /// The integer dimension tuple — every column except `emissionQuant`.
    /// Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the TVV-9 `INSERT … SELECT`
    /// has no `ORDER BY`), so the port sorts purely to make the result
    /// reproducible.
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
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

// ---------------------------------------------------------------------------
// Thin wrapper types for RunSpec* tables stored as Vec<i32> in inputs.
// ---------------------------------------------------------------------------

struct RunSpecHourDayIdRow { hour_day_id: i32 }
struct RunSpecMonthIdRow { month_id: i32 }
struct RunSpecSourceTypeIdRow { source_type_id: i32 }

impl TableRow for RunSpecHourDayIdRow {
    fn table_name() -> &'static str { "RunSpecHourDay" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecHourDay";
        let col = df.column("hourDayID").map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height()).map(|i| {
            Ok(RunSpecHourDayIdRow {
                hour_day_id: col.get(i).ok_or_else(|| row_err(t, i, "hourDayID", "null value".into()))?,
            })
        }).collect()
    }
}

impl TableRow for RunSpecMonthIdRow {
    fn table_name() -> &'static str { "RunSpecMonth" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("monthID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecMonth";
        let col = df.column("monthID").map_err(|e| row_err(t, 0, "monthID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        (0..df.height()).map(|i| {
            Ok(RunSpecMonthIdRow {
                month_id: col.get(i).ok_or_else(|| row_err(t, i, "monthID", "null value".into()))?,
            })
        }).collect()
    }
}

impl TableRow for RunSpecSourceTypeIdRow {
    fn table_name() -> &'static str { "RunSpecSourceType" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecSourceType";
        let col = df.column("sourceTypeID").map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height()).map(|i| {
            Ok(RunSpecSourceTypeIdRow {
                source_type_id: col.get(i).ok_or_else(|| row_err(t, i, "sourceTypeID", "null value".into()))?,
            })
        }).collect()
    }
}

// ---------------------------------------------------------------------------
// TableRow impls for all public row types.
// ---------------------------------------------------------------------------

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

impl TableRow for AverageTankGasolineRow {
    fn table_name() -> &'static str { "AverageTankGasoline" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("ETOHVolume".into(), DataType::Float64),
            ("RVP".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
            Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
            Series::new("ETOHVolume".into(), rows.iter().map(|r| r.etoh_volume).collect::<Vec<f64>>()).into(),
            Series::new("RVP".into(), rows.iter().map(|r| r.rvp).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AverageTankGasoline";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let ft = get_i32("fuelTypeID")?;
        let fy = get_i32("fuelYearID")?;
        let mg = get_i32("monthGroupID")?;
        let etoh = get_f64("ETOHVolume")?;
        let rvp = get_f64("RVP")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(AverageTankGasolineRow {
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                fuel_year_id: fy.get(i).ok_or_else(|| null("fuelYearID"))?,
                month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                etoh_volume: etoh.get(i).ok_or_else(|| null("ETOHVolume"))?,
                rvp: rvp.get(i).ok_or_else(|| null("RVP"))?,
            })
        }).collect()
    }
}

impl TableRow for ColdSoakInitialHourFractionRow {
    fn table_name() -> &'static str { "ColdSoakInitialHourFraction" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("initialHourDayID".into(), DataType::Int32),
            ("coldSoakInitialHourFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("initialHourDayID".into(), rows.iter().map(|r| r.initial_hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("coldSoakInitialHourFraction".into(), rows.iter().map(|r| r.cold_soak_initial_hour_fraction).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ColdSoakInitialHourFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let st = get_i32("sourceTypeID")?;
        let zone = get_i32("zoneID")?;
        let mo = get_i32("monthID")?;
        let hd = get_i32("hourDayID")?;
        let ihd = get_i32("initialHourDayID")?;
        let frac = df.column("coldSoakInitialHourFraction").map_err(|e| row_err(t, 0, "coldSoakInitialHourFraction", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "coldSoakInitialHourFraction", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ColdSoakInitialHourFractionRow {
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                initial_hour_day_id: ihd.get(i).ok_or_else(|| null("initialHourDayID"))?,
                cold_soak_initial_hour_fraction: frac.get(i).ok_or_else(|| null("coldSoakInitialHourFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for ColdSoakTankTemperatureRow {
    fn table_name() -> &'static str { "ColdSoakTankTemperature" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("coldSoakTankTemperature".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
            Series::new("coldSoakTankTemperature".into(), rows.iter().map(|r| r.cold_soak_tank_temperature).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ColdSoakTankTemperature";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mo = get_i32("monthID")?;
        let hr = get_i32("hourID")?;
        let temp = df.column("coldSoakTankTemperature").map_err(|e| row_err(t, 0, "coldSoakTankTemperature", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "coldSoakTankTemperature", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ColdSoakTankTemperatureRow {
                month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                cold_soak_tank_temperature: temp.get(i).ok_or_else(|| null("coldSoakTankTemperature"))?,
            })
        }).collect()
    }
}

impl TableRow for CountyRow {
    fn table_name() -> &'static str { "County" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("altitude".into(), DataType::String),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
            Series::new("altitude".into(), rows.iter().map(|r| r.altitude.to_string()).collect::<Vec<String>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let county_id_col = df.column("countyID").map_err(|e| row_err(t, 0, "countyID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "countyID", e.to_string()))?;
        let altitude_col = df.column("altitude").map_err(|e| row_err(t, 0, "altitude", e.to_string()))?.str().map_err(|e| row_err(t, 0, "altitude", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            let altitude = altitude_col.get(i).ok_or_else(|| null("altitude"))?
                .chars().next().ok_or_else(|| row_err(t, i, "altitude", "empty string".into()))?;
            Ok(CountyRow {
                county_id: county_id_col.get(i).ok_or_else(|| null("countyID"))?,
                altitude,
            })
        }).collect()
    }
}

impl TableRow for CumTvvCoeffsRow {
    fn table_name() -> &'static str { "CumTVVCoeffs" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("regClassID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("tvvTermA".into(), DataType::Float64),
            ("tvvTermB".into(), DataType::Float64),
            ("tvvTermC".into(), DataType::Float64),
            ("tvvTermAIM".into(), DataType::Float64),
            ("tvvTermBIM".into(), DataType::Float64),
            ("tvvTermCIM".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("regClassID".into(), rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
            Series::new("ageGroupID".into(), rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("tvvTermA".into(), rows.iter().map(|r| r.tvv_term_a).collect::<Vec<f64>>()).into(),
            Series::new("tvvTermB".into(), rows.iter().map(|r| r.tvv_term_b).collect::<Vec<f64>>()).into(),
            Series::new("tvvTermC".into(), rows.iter().map(|r| r.tvv_term_c).collect::<Vec<f64>>()).into(),
            Series::new("tvvTermAIM".into(), rows.iter().map(|r| r.tvv_term_a_im).collect::<Vec<f64>>()).into(),
            Series::new("tvvTermBIM".into(), rows.iter().map(|r| r.tvv_term_b_im).collect::<Vec<f64>>()).into(),
            Series::new("tvvTermCIM".into(), rows.iter().map(|r| r.tvv_term_c_im).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "CumTVVCoeffs";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let rc = get_i32("regClassID")?; let myg = get_i32("modelYearGroupID")?;
        let ag = get_i32("ageGroupID")?; let pp = get_i32("polProcessID")?;
        let ta = get_f64("tvvTermA")?; let tb = get_f64("tvvTermB")?; let tc = get_f64("tvvTermC")?;
        let taim = get_f64("tvvTermAIM")?; let tbim = get_f64("tvvTermBIM")?; let tcim = get_f64("tvvTermCIM")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(CumTvvCoeffsRow {
                reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                tvv_term_a: ta.get(i).ok_or_else(|| null("tvvTermA"))?,
                tvv_term_b: tb.get(i).ok_or_else(|| null("tvvTermB"))?,
                tvv_term_c: tc.get(i).ok_or_else(|| null("tvvTermC"))?,
                tvv_term_a_im: taim.get(i).ok_or_else(|| null("tvvTermAIM"))?,
                tvv_term_b_im: tbim.get(i).ok_or_else(|| null("tvvTermBIM"))?,
                tvv_term_c_im: tcim.get(i).ok_or_else(|| null("tvvTermCIM"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let sb = df.column("sourceBinID").map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?.i64().map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let pp = get_i32("polProcessID")?; let om = get_i32("opModeID")?; let ag = get_i32("ageGroupID")?;
        let mbr = get_f64("meanBaseRate")?; let mbrim = get_f64("meanBaseRateIM")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(EmissionRateByAgeRow {
                source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: om.get(i).ok_or_else(|| null("opModeID"))?,
                age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                mean_base_rate: mbr.get(i).ok_or_else(|| null("meanBaseRate"))?,
                mean_base_rate_im: mbrim.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelTypeRow {
    fn table_name() -> &'static str { "FuelType" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("subjectToEvapCalculations".into(), DataType::Boolean),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("subjectToEvapCalculations".into(), rows.iter().map(|r| r.subject_to_evap_calculations).collect::<Vec<bool>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelType";
        let ft = df.column("fuelTypeID").map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        let evap = df.column("subjectToEvapCalculations").map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?.bool().map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelTypeRow {
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                subject_to_evap_calculations: evap.get(i).ok_or_else(|| null("subjectToEvapCalculations"))?,
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
        let hd = get_i32("hourDayID")?; let day = get_i32("dayID")?; let hr = get_i32("hourID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(HourDayRow {
                hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
            })
        }).collect()
    }
}

impl TableRow for ImCoverageRow {
    fn table_name() -> &'static str { "IMCoverage" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("begModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("complianceFactor".into(), DataType::Float64),
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
            Series::new("begModelYearID".into(), rows.iter().map(|r| r.beg_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("endModelYearID".into(), rows.iter().map(|r| r.end_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("complianceFactor".into(), rows.iter().map(|r| r.compliance_factor).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMCoverage";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?; let inf = get_i32("inspectFreq")?; let ts = get_i32("testStandardsID")?;
        let st = get_i32("sourceTypeID")?; let ft = get_i32("fuelTypeID")?;
        let bmy = get_i32("begModelYearID")?; let emy = get_i32("endModelYearID")?;
        let cf = df.column("complianceFactor").map_err(|e| row_err(t, 0, "complianceFactor", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "complianceFactor", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ImCoverageRow {
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                inspect_freq: inf.get(i).ok_or_else(|| null("inspectFreq"))?,
                test_standards_id: ts.get(i).ok_or_else(|| null("testStandardsID"))?,
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                beg_model_year_id: bmy.get(i).ok_or_else(|| null("begModelYearID"))?,
                end_model_year_id: emy.get(i).ok_or_else(|| null("endModelYearID"))?,
                compliance_factor: cf.get(i).ok_or_else(|| null("complianceFactor"))?,
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
        let pp = get_i32("polProcessID")?; let inf = get_i32("inspectFreq")?; let ts = get_i32("testStandardsID")?;
        let st = get_i32("sourceTypeID")?; let ft = get_i32("fuelTypeID")?;
        let imyg = get_i32("IMModelYearGroupID")?; let ag = get_i32("ageGroupID")?;
        let imf = df.column("IMFactor").map_err(|e| row_err(t, 0, "IMFactor", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "IMFactor", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ImFactorRow {
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                inspect_freq: inf.get(i).ok_or_else(|| null("inspectFreq"))?,
                test_standards_id: ts.get(i).ok_or_else(|| null("testStandardsID"))?,
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                im_model_year_group_id: imyg.get(i).ok_or_else(|| null("IMModelYearGroupID"))?,
                age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                im_factor: imf.get(i).ok_or_else(|| null("IMFactor"))?,
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
        let mo = get_i32("monthID")?; let mg = get_i32("monthGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(MonthOfAnyYearRow {
                month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
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
            ("linkID".into(), DataType::Int32),
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
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
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
        let st = get_i32("sourceTypeID")?; let hd = get_i32("hourDayID")?; let lk = get_i32("linkID")?;
        let pp = get_i32("polProcessID")?; let om = get_i32("opModeID")?;
        let omf = df.column("opModeFraction").map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(OpModeDistributionRow {
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                link_id: lk.get(i).ok_or_else(|| null("linkID"))?,
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: om.get(i).ok_or_else(|| null("opModeID"))?,
                op_mode_fraction: omf.get(i).ok_or_else(|| null("opModeFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for PollutantProcessAssocRow {
    fn table_name() -> &'static str { "PollutantProcessAssoc" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
            Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?; let proc = get_i32("processID")?; let poll = get_i32("pollutantID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessAssocRow {
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                pollutant_id: poll.get(i).ok_or_else(|| null("pollutantID"))?,
            })
        }).collect()
    }
}

impl TableRow for PollutantProcessModelYearRow {
    fn table_name() -> &'static str { "PollutantProcessModelYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("IMModelYearGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
            Series::new("IMModelYearGroupID".into(), rows.iter().map(|r| r.im_model_year_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?; let my = get_i32("modelYearID")?;
        let myg = get_i32("modelYearGroupID")?; let imyg = get_i32("IMModelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessModelYearRow {
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                im_model_year_group_id: imyg.get(i).ok_or_else(|| null("IMModelYearGroupID"))?,
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
            ("regClassID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("regClassID".into(), rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBin";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let sb = df.column("sourceBinID").map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?.i64().map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let ft = get_i32("fuelTypeID")?; let rc = get_i32("regClassID")?; let myg = get_i32("modelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinRow {
                source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
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
        let stmy = get_i32("sourceTypeModelYearID")?; let pp = get_i32("polProcessID")?;
        let sb = df.column("sourceBinID").map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?.i64().map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let sbaf = df.column("sourceBinActivityFraction").map_err(|e| row_err(t, 0, "sourceBinActivityFraction", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "sourceBinActivityFraction", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinDistributionRow {
                source_type_model_year_id: stmy.get(i).ok_or_else(|| null("sourceTypeModelYearID"))?,
                pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                source_bin_activity_fraction: sbaf.get(i).ok_or_else(|| null("sourceBinActivityFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str { "SourceHours" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceHours".into(), rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceHours";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hd = get_i32("hourDayID")?; let mo = get_i32("monthID")?; let age = get_i32("ageID")?; let st = get_i32("sourceTypeID")?;
        let sh = df.column("sourceHours").map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceHoursRow {
                hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                source_hours: sh.get(i).ok_or_else(|| null("sourceHours"))?,
            })
        }).collect()
    }
}

impl TableRow for SourceTypeModelYearRow {
    fn table_name() -> &'static str { "SourceTypeModelYear" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceTypeModelYearID".into(), rows.iter().map(|r| r.source_type_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?; let my = get_i32("modelYearID")?; let st = get_i32("sourceTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceTypeModelYearRow {
                source_type_model_year_id: stmy.get(i).ok_or_else(|| null("sourceTypeModelYearID"))?,
                model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for TankVaporGenCoeffsRow {
    fn table_name() -> &'static str { "TankVaporGenCoeffs" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("ethanolLevelID".into(), DataType::Int32),
            ("altitude".into(), DataType::String),
            ("tvgTermA".into(), DataType::Float64),
            ("tvgTermB".into(), DataType::Float64),
            ("tvgTermC".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("ethanolLevelID".into(), rows.iter().map(|r| r.ethanol_level_id).collect::<Vec<i32>>()).into(),
            Series::new("altitude".into(), rows.iter().map(|r| r.altitude.to_string()).collect::<Vec<String>>()).into(),
            Series::new("tvgTermA".into(), rows.iter().map(|r| r.tvg_term_a).collect::<Vec<f64>>()).into(),
            Series::new("tvgTermB".into(), rows.iter().map(|r| r.tvg_term_b).collect::<Vec<f64>>()).into(),
            Series::new("tvgTermC".into(), rows.iter().map(|r| r.tvg_term_c).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "TankVaporGenCoeffs";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let el = get_i32("ethanolLevelID")?;
        let alt_col = df.column("altitude").map_err(|e| row_err(t, 0, "altitude", e.to_string()))?.str().map_err(|e| row_err(t, 0, "altitude", e.to_string()))?;
        let ta = get_f64("tvgTermA")?; let tb = get_f64("tvgTermB")?; let tc = get_f64("tvgTermC")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            let altitude = alt_col.get(i).ok_or_else(|| null("altitude"))?
                .chars().next().ok_or_else(|| row_err(t, i, "altitude", "empty string".into()))?;
            Ok(TankVaporGenCoeffsRow {
                ethanol_level_id: el.get(i).ok_or_else(|| null("ethanolLevelID"))?,
                altitude,
                tvg_term_a: ta.get(i).ok_or_else(|| null("tvgTermA"))?,
                tvg_term_b: tb.get(i).ok_or_else(|| null("tvgTermB"))?,
                tvg_term_c: tc.get(i).ok_or_else(|| null("tvgTermC"))?,
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
        let yr = get_i32("yearID")?; let fy = get_i32("fuelYearID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(YearRow {
                year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                fuel_year_id: fy.get(i).ok_or_else(|| null("fuelYearID"))?,
            })
        }).collect()
    }
}

impl TableRow for ZoneRow {
    fn table_name() -> &'static str { "Zone" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Zone";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone = get_i32("zoneID")?; let county = get_i32("countyID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ZoneRow {
                zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                county_id: county.get(i).ok_or_else(|| null("countyID"))?,
            })
        }).collect()
    }
}

impl TableRow for TankVaporVentingEmissionRow {
    fn table_name() -> &'static str { "MOVESWorkerOutput" }
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
        DataFrame::new(n, vec![
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
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
            Series::new("emissionQuant".into(), rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let yr = get_i32("yearID")?; let mo = get_i32("monthID")?; let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?; let state = get_i32("stateID")?; let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?; let link = get_i32("linkID")?; let poll = get_i32("pollutantID")?;
        let proc = get_i32("processID")?; let st = get_i32("sourceTypeID")?; let ft = get_i32("fuelTypeID")?;
        let my = get_i32("modelYearID")?; let rt = get_i32("roadTypeID")?;
        let eq = df.column("emissionQuant").map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?.f64().map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(TankVaporVentingEmissionRow {
                year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                pollutant_id: poll.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                road_type_id: rt.get(i).ok_or_else(|| null("roadTypeID"))?,
                emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
            })
        }).collect()
    }
}

// ===========================================================================
// Working tables — private mirrors of the nine MyISAM tables the SQL's
// "Processing" section builds and drops. Each numbered TVV step produces
// one (TVV-7 builds `HourlyTVV` from two scratch tables folded into the
// step function); later steps consume it.
// ===========================================================================

/// TVV-1 working table — `IMCoverageMergedUngrouped`. The summed I/M
/// adjustment fraction per `(process, pollutant, modelYear, fuelType,
/// sourceType)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ImCoverageMerged {
    process_id: i32,
    pollutant_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    source_type_id: i32,
    im_adjust_fract: f64,
}

/// TVV-2 working table — `PeakHourOfColdSoak`. The hour of peak cold-soak
/// tank temperature for each month.
#[derive(Debug, Clone, Copy, PartialEq)]
struct PeakHourOfColdSoak {
    month_id: i32,
    peak_hour_id: i32,
}

/// TVV-3 working table — `TankVaporGenerated`. Tank vapor generated for one
/// `(currentHour, initialHour)` cold-soak pair at one ethanol level.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TankVaporGenerated {
    hour_day_id: i32,
    initial_hour_day_id: i32,
    ethanol_level_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_year_id: i32,
    fuel_type_id: i32,
    tank_vapor_generated: f64,
}

/// TVV-4 working table — `EthanolWeightedTVG`. Tank vapor generated,
/// linearly interpolated between the 0- and 10-percent ethanol levels by
/// the fuel's ethanol volume.
#[derive(Debug, Clone, Copy, PartialEq)]
struct EthanolWeightedTvg {
    hour_day_id: i32,
    initial_hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_year_id: i32,
    fuel_type_id: i32,
    ethanol_weighted_tvg: f64,
}

/// TVV-5 working table — `CummulativeTankVaporVented`. Cumulative tank
/// vapor vented (base and I/M) by regulatory class and vehicle age, with
/// the prior hour pre-computed for TVV-6's self-join.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CumulativeTankVaporVented {
    reg_class_id: i32,
    age_id: i32,
    pol_process_id: i32,
    day_id: i32,
    hour_id: i32,
    initial_hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    tank_vapor_vented: f64,
    tank_vapor_vented_im: f64,
    hour_day_id: i32,
    prior_hour_id: i32,
}

/// TVV-6 working table — `UnweightedHourlyTVV`. The hour-over-hour increment
/// of cumulative TVV, floored at zero.
#[derive(Debug, Clone, Copy, PartialEq)]
struct UnweightedHourlyTvv {
    zone_id: i32,
    reg_class_id: i32,
    age_id: i32,
    pol_process_id: i32,
    hour_day_id: i32,
    initial_hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    unweighted_hourly_tvv: f64,
    unweighted_hourly_tvv_im: f64,
}

/// TVV-7 working table — `HourlyTVV`. The cold-soak-fraction-weighted hourly
/// TVV, summed across the initial-hour dimension, plus the four post-peak
/// decay hours.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HourlyTvv {
    reg_class_id: i32,
    age_id: i32,
    pol_process_id: i32,
    hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    hourly_tvv: f64,
    hourly_tvv_im: f64,
}

/// TVV-8 working table — `WeightedMeanBaseRate`. The source-bin-activity-
/// weighted mean base rate (base and I/M) per operating mode.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedMeanBaseRate {
    pol_process_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    month_id: i32,
    hour_day_id: i32,
    model_year_id: i32,
    op_mode_id: i32,
    weighted_mean_base_rate: f64,
    weighted_mean_base_rate_im: f64,
}

/// The five-column key TVV-1 joins `IMCoverage` and `IMFactor` on:
/// `(polProcessID, inspectFreq, testStandardsID, sourceTypeID, fuelTypeID)`.
type ImJoinKey = (i32, i32, i32, i32, i32);

/// The five-column group-by / output key of one `IMCoverageMergedUngrouped`
/// row: `(processID, pollutantID, modelYearID, fuelTypeID, sourceTypeID)`.
type ImMergedKey = (i32, i32, i32, i32, i32);

/// TVV-1 — complete the I/M adjustment fraction information.
///
/// For each `(process, pollutant, modelYear, fuelType, sourceType)` the
/// step sums `IMFactor × complianceFactor × 0.01` over every I/M coverage
/// row that applies. `PollutantProcessModelYear` is joined to
/// `PollutantProcessAssoc` (`polProcessID`), to `IMFactor`
/// (`polProcessID, IMModelYearGroupID`), to `AgeCategory`
/// (`ageGroupID`, with the model year pinned to `year − ageID`), and to
/// `IMCoverage` (the five-column [`ImJoinKey`] plus the coverage's
/// `[begModelYearID, endModelYearID]` model-year range).
///
/// The SQL's `IMCoverage.countyID` / `IMCoverage.yearID` `WHERE` clauses and
/// the `polProcessID IN (…)` clause repeat the "Extract Data" filters and
/// are omitted (see the module scope notes); the `modelYearID = year −
/// ageID` clause is a genuine join condition and is ported.
fn im_coverage_merged_ungrouped(
    inputs: &TankVaporVentingInputs,
    ctx: &RunContext,
) -> Vec<ImCoverageMerged> {
    // PollutantProcessAssoc is keyed on polProcessID — one row per id.
    let assoc_by_pp: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
    let mut factor_by_key: HashMap<(i32, i32), Vec<&ImFactorRow>> = HashMap::new();
    for imf in &inputs.im_factor {
        factor_by_key
            .entry((imf.pol_process_id, imf.im_model_year_group_id))
            .or_default()
            .push(imf);
    }
    let mut age_by_group: HashMap<i32, Vec<&AgeCategoryRow>> = HashMap::new();
    for ac in &inputs.age_category {
        age_by_group.entry(ac.age_group_id).or_default().push(ac);
    }
    let mut coverage_by_key: HashMap<ImJoinKey, Vec<&ImCoverageRow>> = HashMap::new();
    for imc in &inputs.im_coverage {
        coverage_by_key
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
    let mut merged: HashMap<ImMergedKey, f64> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        let Some(assoc) = assoc_by_pp.get(&ppmy.pol_process_id) else {
            continue;
        };
        let Some(factors) = factor_by_key.get(&(assoc.pol_process_id, ppmy.im_model_year_group_id))
        else {
            continue;
        };
        for imf in factors {
            let Some(ages) = age_by_group.get(&imf.age_group_id) else {
                continue;
            };
            for ac in ages {
                // WHERE ppmy.modelYearID = ##context.year## - ageID.
                if ppmy.model_year_id != ctx.year - ac.age_id {
                    continue;
                }
                let Some(coverages) = coverage_by_key.get(&(
                    imf.pol_process_id,
                    imf.inspect_freq,
                    imf.test_standards_id,
                    imf.source_type_id,
                    imf.fuel_type_id,
                )) else {
                    continue;
                };
                for imc in coverages {
                    // INNER JOIN IMCoverage … begModelYearID <= modelYearID
                    // <= endModelYearID.
                    if imc.beg_model_year_id > ppmy.model_year_id
                        || imc.end_model_year_id < ppmy.model_year_id
                    {
                        continue;
                    }
                    *merged
                        .entry((
                            assoc.process_id,
                            assoc.pollutant_id,
                            ppmy.model_year_id,
                            imf.fuel_type_id,
                            imc.source_type_id,
                        ))
                        .or_default() += imf.im_factor * imc.compliance_factor * 0.01;
                }
            }
        }
    }

    merged
        .into_iter()
        .map(
            |((process_id, pollutant_id, model_year_id, fuel_type_id, source_type_id), fract)| {
                ImCoverageMerged {
                    process_id,
                    pollutant_id,
                    model_year_id,
                    fuel_type_id,
                    source_type_id,
                    im_adjust_fract: fract,
                }
            },
        )
        .collect()
}

/// TVV-2 — determine the hour of peak cold-soak tank temperature.
///
/// For each `monthID`, `peakHourID` is the hour whose cold-soak tank
/// temperature, rounded to two decimal places, is highest — ties broken to
/// the earliest hour. The SQL packs that ranking into one `max` —
/// `round(T,2)*100000 + (999 − hourID)`, unpacked with `mod(…,1000)`. The
/// port ranks each row by the exact-integer tuple
/// `(round(T × 100), −hourID)`, which is the same ordering without the
/// float noise the SQL's `mod` step is exposed to (see the module fidelity
/// notes).
fn peak_hour_of_cold_soak(inputs: &TankVaporVentingInputs) -> Vec<PeakHourOfColdSoak> {
    // best[monthID] = (roundedTempScaled, -hourID): a higher rounded
    // temperature wins; on a tie the smaller hourID (larger -hourID) wins.
    let mut best: HashMap<i32, (i64, i32)> = HashMap::new();
    for row in &inputs.cold_soak_tank_temperature {
        let rounded = (row.cold_soak_tank_temperature * 100.0).round() as i64;
        let candidate = (rounded, -row.hour_id);
        best.entry(row.month_id)
            .and_modify(|current| {
                if candidate > *current {
                    *current = candidate;
                }
            })
            .or_insert(candidate);
    }
    let mut out: Vec<PeakHourOfColdSoak> = best
        .into_iter()
        .map(|(month_id, (_, neg_hour))| PeakHourOfColdSoak {
            month_id,
            peak_hour_id: -neg_hour,
        })
        .collect();
    out.sort_unstable_by_key(|p| p.month_id);
    out
}

/// TVV-3 — calculate `TankVaporGenerated` (TVG) by ethanol level.
///
/// For each `ColdSoakInitialHourFraction` row with a positive fraction and a
/// current hour-day distinct from its initial hour-day, the step resolves
/// the current-hour temperature `t2` and the initial-hour temperature `t1`,
/// keeps only hours up to the month's peak cold-soak hour, looks up the
/// county altitude (via `Zone` → `County`) to select the
/// `TankVaporGenCoeffs`, and the month group (via `MonthOfAnyYear`) to
/// select `AverageTankGasoline`'s `RVP`. The vapor generated is
///
/// ```text
/// TVG = tvgTermA · exp(tvgTermB · RVP)
///       · (exp(tvgTermC · t2) − exp(tvgTermC · t1))
/// ```
///
/// clamped to `0` whenever the initial-hour temperature is at or above the
/// current-hour temperature (the tank is warming, not cooling). The SQL's
/// leading `1.0 *` factor — its "`k`" constant — is `1.0` and is elided.
fn tank_vapor_generated(
    inputs: &TankVaporVentingInputs,
    peak_hours: &[PeakHourOfColdSoak],
) -> Vec<TankVaporGenerated> {
    // HourDay is keyed on hourDayID — one hourID per id.
    let hour_of: HashMap<i32, i32> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd.hour_id))
        .collect();
    let peak_of: HashMap<i32, i32> = peak_hours
        .iter()
        .map(|p| (p.month_id, p.peak_hour_id))
        .collect();
    // ColdSoakTankTemperature is extracted to one zone — (monthID, hourID)
    // is unique.
    let temp_of: HashMap<(i32, i32), f64> = inputs
        .cold_soak_tank_temperature
        .iter()
        .map(|r| ((r.month_id, r.hour_id), r.cold_soak_tank_temperature))
        .collect();
    let county_of_zone: HashMap<i32, i32> = inputs
        .zone
        .iter()
        .map(|z| (z.zone_id, z.county_id))
        .collect();
    let altitude_of_county: HashMap<i32, char> = inputs
        .county
        .iter()
        .map(|c| (c.county_id, c.altitude))
        .collect();
    let mut coeffs_by_altitude: HashMap<char, Vec<&TankVaporGenCoeffsRow>> = HashMap::new();
    for c in &inputs.tank_vapor_gen_coeffs {
        coeffs_by_altitude.entry(c.altitude).or_default().push(c);
    }
    let month_group_of: HashMap<i32, i32> = inputs
        .month_of_any_year
        .iter()
        .map(|m| (m.month_id, m.month_group_id))
        .collect();
    let mut gasoline_by_group: HashMap<i32, Vec<&AverageTankGasolineRow>> = HashMap::new();
    for g in &inputs.average_tank_gasoline {
        gasoline_by_group
            .entry(g.month_group_id)
            .or_default()
            .push(g);
    }

    let mut out = Vec::new();
    for ihf in &inputs.cold_soak_initial_hour_fraction {
        // WHERE coldSoakInitialHourFraction > 0 AND hourDayID <> initialHourDayID.
        if ihf.cold_soak_initial_hour_fraction <= 0.0 || ihf.hour_day_id == ihf.initial_hour_day_id
        {
            continue;
        }
        let (Some(&current_hour), Some(&initial_hour)) = (
            hour_of.get(&ihf.hour_day_id),
            hour_of.get(&ihf.initial_hour_day_id),
        ) else {
            continue;
        };
        let Some(&peak_hour) = peak_of.get(&ihf.month_id) else {
            continue;
        };
        // WHERE hd.hourID <= ph.peakHourID.
        if current_hour > peak_hour {
            continue;
        }
        let Some(&t2) = temp_of.get(&(ihf.month_id, current_hour)) else {
            continue;
        };
        let Some(&t1) = temp_of.get(&(ihf.month_id, initial_hour)) else {
            continue;
        };
        let Some(&county_id) = county_of_zone.get(&ihf.zone_id) else {
            continue;
        };
        let Some(&altitude) = altitude_of_county.get(&county_id) else {
            continue;
        };
        let Some(coeffs_list) = coeffs_by_altitude.get(&altitude) else {
            continue;
        };
        let Some(&month_group_id) = month_group_of.get(&ihf.month_id) else {
            continue;
        };
        let Some(gasoline_list) = gasoline_by_group.get(&month_group_id) else {
            continue;
        };
        for coeffs in coeffs_list {
            for gas in gasoline_list {
                let tvg = if t1 >= t2 {
                    0.0
                } else {
                    coeffs.tvg_term_a
                        * (coeffs.tvg_term_b * gas.rvp).exp()
                        * ((coeffs.tvg_term_c * t2).exp() - (coeffs.tvg_term_c * t1).exp())
                };
                out.push(TankVaporGenerated {
                    hour_day_id: ihf.hour_day_id,
                    initial_hour_day_id: ihf.initial_hour_day_id,
                    ethanol_level_id: coeffs.ethanol_level_id,
                    month_id: ihf.month_id,
                    source_type_id: ihf.source_type_id,
                    fuel_year_id: gas.fuel_year_id,
                    fuel_type_id: gas.fuel_type_id,
                    tank_vapor_generated: tvg,
                });
            }
        }
    }
    out
}

/// The six-column key TVV-4 joins the ethanol-level-0 and ethanol-level-10
/// `TankVaporGenerated` rows on: `(hourDayID, initialHourDayID, monthID,
/// sourceTypeID, fuelYearID, fuelTypeID)`.
type TvgPairKey = (i32, i32, i32, i32, i32, i32);

/// TVV-4 — calculate ethanol-weighted TVG.
///
/// Each ethanol-level-0 `TankVaporGenerated` row is paired with its
/// ethanol-level-10 sibling (same [`TvgPairKey`]) and linearly interpolated
/// by the fuel's ethanol volume:
///
/// ```text
/// ethanolWeightedTVG = TVG₁₀ · f + TVG₀ · (1 − f),
///   where f = least(10, ETOHVolume) / 10
/// ```
///
/// `ETOHVolume` is read from `AverageTankGasoline`, joined through
/// `MonthOfAnyYear` on the level-10 row's `(monthID, fuelYearID,
/// fuelTypeID)`.
fn ethanol_weighted_tvg(
    inputs: &TankVaporVentingInputs,
    generated: &[TankVaporGenerated],
) -> Vec<EthanolWeightedTvg> {
    let mut level_ten: HashMap<TvgPairKey, &TankVaporGenerated> = HashMap::new();
    for g in generated {
        if g.ethanol_level_id == 10 {
            level_ten.insert(
                (
                    g.hour_day_id,
                    g.initial_hour_day_id,
                    g.month_id,
                    g.source_type_id,
                    g.fuel_year_id,
                    g.fuel_type_id,
                ),
                g,
            );
        }
    }
    let month_group_of: HashMap<i32, i32> = inputs
        .month_of_any_year
        .iter()
        .map(|m| (m.month_id, m.month_group_id))
        .collect();
    let etoh_of: HashMap<(i32, i32, i32), f64> = inputs
        .average_tank_gasoline
        .iter()
        .map(|g| {
            (
                (g.month_group_id, g.fuel_year_id, g.fuel_type_id),
                g.etoh_volume,
            )
        })
        .collect();

    let mut out = Vec::new();
    for t0 in generated {
        if t0.ethanol_level_id != 0 {
            continue;
        }
        let key = (
            t0.hour_day_id,
            t0.initial_hour_day_id,
            t0.month_id,
            t0.source_type_id,
            t0.fuel_year_id,
            t0.fuel_type_id,
        );
        let Some(t10) = level_ten.get(&key) else {
            continue;
        };
        let Some(&month_group_id) = month_group_of.get(&t10.month_id) else {
            continue;
        };
        let Some(&etoh_volume) = etoh_of.get(&(month_group_id, t10.fuel_year_id, t10.fuel_type_id))
        else {
            continue;
        };
        let fraction = etoh_volume.min(10.0) / 10.0;
        let weighted =
            t10.tank_vapor_generated * fraction + t0.tank_vapor_generated * (1.0 - fraction);
        out.push(EthanolWeightedTvg {
            hour_day_id: t0.hour_day_id,
            initial_hour_day_id: t0.initial_hour_day_id,
            month_id: t0.month_id,
            source_type_id: t0.source_type_id,
            fuel_year_id: t0.fuel_year_id,
            fuel_type_id: t0.fuel_type_id,
            ethanol_weighted_tvg: weighted,
        });
    }
    out
}

/// TVV-5 — calculate cumulative tank vapor vented (TVV).
///
/// For each `CumTVVCoeffs` row the step cross-joins every
/// [`EthanolWeightedTvg`] row (the SQL's `inner join EthanolWeightedTVG`
/// carries no `ON` clause, so it is a cartesian product), resolves the
/// coefficient row's age group through `AgeCategory` and its model-year
/// group through `PollutantProcessModelYear`, and keeps the
/// `(age, modelYear)` pairs satisfying `ageID = year − modelYearID`.
/// Cumulative vapor vented is the quadratic
///
/// ```text
/// TVV = greatest(tvvTermA + ewTVG · (tvvTermB + tvvTermC · ewTVG), 0)
/// ```
///
/// evaluated once with the base `tvvTerm*` coefficients and once with the
/// I/M `tvvTerm*IM` coefficients. `priorHourID` is the cyclic predecessor
/// of `hourID` within `1..=24` — `mod(hourID − 1 − 1 + 24, 24) + 1` — and
/// is pre-computed here for TVV-6's prior-hour self-join.
fn cumulative_tank_vapor_vented(
    inputs: &TankVaporVentingInputs,
    weighted: &[EthanolWeightedTvg],
) -> Vec<CumulativeTankVaporVented> {
    let mut age_by_group: HashMap<i32, Vec<&AgeCategoryRow>> = HashMap::new();
    for ac in &inputs.age_category {
        age_by_group.entry(ac.age_group_id).or_default().push(ac);
    }
    let mut ppmy_by_group: HashMap<(i32, i32), Vec<&PollutantProcessModelYearRow>> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        ppmy_by_group
            .entry((ppmy.pol_process_id, ppmy.model_year_group_id))
            .or_default()
            .push(ppmy);
    }
    // HourDay is keyed on hourDayID — one (dayID, hourID) per id.
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let mut year_by_fuel_year: HashMap<i32, Vec<&YearRow>> = HashMap::new();
    for y in &inputs.year {
        year_by_fuel_year.entry(y.fuel_year_id).or_default().push(y);
    }

    let mut out = Vec::new();
    for coeffs in &inputs.cum_tvv_coeffs {
        let Some(ages) = age_by_group.get(&coeffs.age_group_id) else {
            continue;
        };
        let Some(model_years) =
            ppmy_by_group.get(&(coeffs.pol_process_id, coeffs.model_year_group_id))
        else {
            continue;
        };
        for ew in weighted {
            let Some(hd) = hour_day_of.get(&ew.hour_day_id) else {
                continue;
            };
            let Some(years) = year_by_fuel_year.get(&ew.fuel_year_id) else {
                continue;
            };
            let tvg = ew.ethanol_weighted_tvg;
            let tvv =
                (coeffs.tvv_term_a + tvg * (coeffs.tvv_term_b + coeffs.tvv_term_c * tvg)).max(0.0);
            let tvv_im = (coeffs.tvv_term_a_im
                + tvg * (coeffs.tvv_term_b_im + coeffs.tvv_term_c_im * tvg))
                .max(0.0);
            // mod(hourID - 1 - 1 + 24, 24) + 1 — the cyclic prior hour.
            let prior_hour_id = (hd.hour_id - 1 - 1 + 24).rem_euclid(24) + 1;
            for y in years {
                for ac in ages {
                    for ppmy in model_years {
                        // WHERE acat.ageID = y.yearID - ppmy.modelYearID.
                        if ac.age_id != y.year_id - ppmy.model_year_id {
                            continue;
                        }
                        out.push(CumulativeTankVaporVented {
                            reg_class_id: coeffs.reg_class_id,
                            age_id: ac.age_id,
                            pol_process_id: coeffs.pol_process_id,
                            day_id: hd.day_id,
                            hour_id: hd.hour_id,
                            initial_hour_day_id: ew.initial_hour_day_id,
                            month_id: ew.month_id,
                            source_type_id: ew.source_type_id,
                            fuel_type_id: ew.fuel_type_id,
                            tank_vapor_vented: tvv,
                            tank_vapor_vented_im: tvv_im,
                            hour_day_id: ew.hour_day_id,
                            prior_hour_id,
                        });
                    }
                }
            }
        }
    }
    // Sorted on the SQL primary key for a canonical, reproducible order.
    out.sort_unstable_by_key(|c| {
        (
            c.reg_class_id,
            c.age_id,
            c.pol_process_id,
            c.day_id,
            c.hour_id,
            c.initial_hour_day_id,
            c.month_id,
            c.source_type_id,
            c.fuel_type_id,
        )
    });
    out
}

/// TVV-6 — calculate unweighted hourly TVV by regulatory class and age.
///
/// The SQL `LEFT JOIN`s `CummulativeTankVaporVented` onto itself: for each
/// "current" row it finds the "prior hour" row carrying the same
/// `(regClassID, ageID, polProcessID, initialHourDayID, monthID,
/// sourceTypeID, fuelTypeID, dayID)` and `hourID = priorHourID`. The hourly
/// increment is `greatest(currentTVV − priorTVV, 0)`, and a `LEFT JOIN`
/// miss contributes `coalesce(priorTVV, 0) = 0`. Those nine columns are the
/// `CummulativeTankVaporVented` primary key, so the prior-hour row is
/// unique and the self-join is a [`HashMap`] lookup. `zoneID` is the
/// `UnweightedHourlyTVV` column default — the run's single zone.
fn unweighted_hourly_tvv(
    cumulative: &[CumulativeTankVaporVented],
    zone_id: i32,
) -> Vec<UnweightedHourlyTvv> {
    // The full CummulativeTankVaporVented primary key.
    type CtvKey = (i32, i32, i32, i32, i32, i32, i32, i32, i32);
    let key_of = |c: &CumulativeTankVaporVented, hour_id: i32| -> CtvKey {
        (
            c.reg_class_id,
            c.age_id,
            c.pol_process_id,
            c.initial_hour_day_id,
            c.month_id,
            c.source_type_id,
            c.fuel_type_id,
            c.day_id,
            hour_id,
        )
    };
    let by_key: HashMap<CtvKey, &CumulativeTankVaporVented> = cumulative
        .iter()
        .map(|c| (key_of(c, c.hour_id), c))
        .collect();

    let mut out: Vec<UnweightedHourlyTvv> = cumulative
        .iter()
        .map(|ctv1| {
            // Prior-hour row: same dimensions, hourID = ctv1.priorHourID.
            let prior = by_key.get(&key_of(ctv1, ctv1.prior_hour_id));
            let prior_tvv = prior.map_or(0.0, |p| p.tank_vapor_vented);
            let prior_tvv_im = prior.map_or(0.0, |p| p.tank_vapor_vented_im);
            UnweightedHourlyTvv {
                zone_id,
                reg_class_id: ctv1.reg_class_id,
                age_id: ctv1.age_id,
                pol_process_id: ctv1.pol_process_id,
                hour_day_id: ctv1.hour_day_id,
                initial_hour_day_id: ctv1.initial_hour_day_id,
                month_id: ctv1.month_id,
                source_type_id: ctv1.source_type_id,
                fuel_type_id: ctv1.fuel_type_id,
                unweighted_hourly_tvv: (ctv1.tank_vapor_vented - prior_tvv).max(0.0),
                unweighted_hourly_tvv_im: (ctv1.tank_vapor_vented_im - prior_tvv_im).max(0.0),
            }
        })
        .collect();
    out.sort_unstable_by_key(|u| {
        (
            u.zone_id,
            u.reg_class_id,
            u.age_id,
            u.pol_process_id,
            u.hour_day_id,
            u.initial_hour_day_id,
            u.month_id,
            u.source_type_id,
            u.fuel_type_id,
        )
    });
    out
}

/// The five-column key TVV-7 part A joins `UnweightedHourlyTVV` to
/// `ColdSoakInitialHourFraction` on: `(sourceTypeID, zoneID, monthID,
/// hourDayID, initialHourDayID)`.
type CsihfKey = (i32, i32, i32, i32, i32);

/// The seven-column `HourlyTVV` primary / group key: `(regClassID, ageID,
/// polProcessID, hourDayID, monthID, sourceTypeID, fuelTypeID)`.
type HourlyTvvKey = (i32, i32, i32, i32, i32, i32, i32);

/// The [`HourlyTvv`] primary-key tuple, for canonical sorting.
fn hourly_tvv_key(h: &HourlyTvv) -> HourlyTvvKey {
    (
        h.reg_class_id,
        h.age_id,
        h.pol_process_id,
        h.hour_day_id,
        h.month_id,
        h.source_type_id,
        h.fuel_type_id,
    )
}

/// TVV-7 — calculate weighted hourly TVV across the initial/current pair.
///
/// Part A weights each [`UnweightedHourlyTvv`] row by its matching
/// `ColdSoakInitialHourFraction` (the SQL's `HourlyTVVTemp` insert) and
/// sums across the `initialHourDayID` dimension, producing one row per
/// [`HourlyTvvKey`] for the hours up to the peak cold-soak hour.
///
/// Part B handles the four post-peak decay hours: each part-A row sitting
/// *at* its month's peak hour (`HourDay`'s `hourID = peakHourID`) seeds
/// rows for hours `peak + 1 ..= peak + 4` on the same `dayID`, scaled by a
/// fixed decay schedule — `0.0200, 0.0100, 0.0040, 0.0005`. The SQL reads
/// these from a `CopyOfHourlyTVV` snapshot taken *before* part B's insert,
/// so the decay rows derive from part A alone; the port mirrors that by
/// computing part B from the part-A vector. Hours past 24 have no
/// `HourDay` row and drop out, exactly as the SQL's `inner join HourDay`
/// finds nothing for them.
fn hourly_tvv(
    inputs: &TankVaporVentingInputs,
    unweighted: &[UnweightedHourlyTvv],
    peak_hours: &[PeakHourOfColdSoak],
) -> Vec<HourlyTvv> {
    // --- Part A: weight by cold-soak fraction, sum over initialHourDayID ---
    let fraction_of: HashMap<CsihfKey, f64> = inputs
        .cold_soak_initial_hour_fraction
        .iter()
        .map(|r| {
            (
                (
                    r.source_type_id,
                    r.zone_id,
                    r.month_id,
                    r.hour_day_id,
                    r.initial_hour_day_id,
                ),
                r.cold_soak_initial_hour_fraction,
            )
        })
        .collect();
    let mut grouped: HashMap<HourlyTvvKey, (f64, f64)> = HashMap::new();
    for u in unweighted {
        let Some(&fraction) = fraction_of.get(&(
            u.source_type_id,
            u.zone_id,
            u.month_id,
            u.hour_day_id,
            u.initial_hour_day_id,
        )) else {
            continue;
        };
        let entry = grouped
            .entry((
                u.reg_class_id,
                u.age_id,
                u.pol_process_id,
                u.hour_day_id,
                u.month_id,
                u.source_type_id,
                u.fuel_type_id,
            ))
            .or_insert((0.0, 0.0));
        entry.0 += u.unweighted_hourly_tvv * fraction;
        entry.1 += u.unweighted_hourly_tvv_im * fraction;
    }
    let mut part_a: Vec<HourlyTvv> = grouped
        .into_iter()
        .map(|(key, (tvv, tvv_im))| HourlyTvv {
            reg_class_id: key.0,
            age_id: key.1,
            pol_process_id: key.2,
            hour_day_id: key.3,
            month_id: key.4,
            source_type_id: key.5,
            fuel_type_id: key.6,
            hourly_tvv: tvv,
            hourly_tvv_im: tvv_im,
        })
        .collect();
    part_a.sort_unstable_by_key(hourly_tvv_key);

    // --- Part B: the four post-peak decay hours ---
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let hour_day_by_day_hour: HashMap<(i32, i32), i32> = inputs
        .hour_day
        .iter()
        .map(|hd| ((hd.day_id, hd.hour_id), hd.hour_day_id))
        .collect();
    let peak_of: HashMap<i32, i32> = peak_hours
        .iter()
        .map(|p| (p.month_id, p.peak_hour_id))
        .collect();

    let mut part_b = Vec::new();
    for htvv in &part_a {
        // CopyOfHourlyTVV = HourlyTVV inner join HourDay on hourDayID.
        let Some(hd) = hour_day_of.get(&htvv.hour_day_id) else {
            continue;
        };
        // inner join PeakHourOfColdSoak on monthID, and hourID = peakHourID.
        let Some(&peak) = peak_of.get(&htvv.month_id) else {
            continue;
        };
        if hd.hour_id != peak {
            continue;
        }
        for offset in 1..=4 {
            let Some(&hour_day_id) = hour_day_by_day_hour.get(&(hd.day_id, peak + offset)) else {
                continue;
            };
            let scale = match offset {
                1 => 0.0200,
                2 => 0.0100,
                3 => 0.0040,
                _ => 0.0005,
            };
            part_b.push(HourlyTvv {
                reg_class_id: htvv.reg_class_id,
                age_id: htvv.age_id,
                pol_process_id: htvv.pol_process_id,
                hour_day_id,
                month_id: htvv.month_id,
                source_type_id: htvv.source_type_id,
                fuel_type_id: htvv.fuel_type_id,
                hourly_tvv: htvv.hourly_tvv * scale,
                hourly_tvv_im: htvv.hourly_tvv_im * scale,
            });
        }
    }

    let mut out = part_a;
    out.extend(part_b);
    out.sort_unstable_by_key(hourly_tvv_key);
    out
}

/// The seven-column `WeightedMeanBaseRate` primary / group key:
/// `(polProcessID, sourceTypeID, fuelTypeID, monthID, hourDayID,
/// modelYearID, opModeID)`.
type WeightedRateKey = (i32, i32, i32, i32, i32, i32, i32);

/// TVV-8 — calculate the I/M-adjusted mean base rates.
///
/// `WeightedMeanBaseRate` is filled by two independent `INSERT … SELECT`
/// statements writing disjoint operating modes:
///
/// * The **cold-soak** insert (`opModeID = 151`) carries the venting
///   chain's [`HourlyTvv`] forward, weighting `hourlyTVV` by
///   `sourceBinActivityFraction` and summing per `(polProcessID,
///   sourceTypeID, fuelTypeID, monthID, hourDayID, modelYearID)`.
/// * The **operating / hot-soak** insert (`opModeID ∈ {150, 300}`) weights
///   `EmissionRateByAge.meanBaseRate` by `sourceBinActivityFraction`,
///   cross-joining `RunSpecMonth` and `RunSpecHourDay` so the
///   month/hour-independent rate is replicated across every run-spec
///   `(monthID, hourDayID)`.
///
/// Both inserts keep only fuel types with `subjectToEvapCalculations = 'Y'`
/// and require a `PollutantProcessModelYear` row linking the source bin's
/// model-year group. The `opModeID IN (150, 300)` filter on
/// `EmissionRateByAge` repeats its extract filter and is omitted (see the
/// module scope notes).
fn weighted_mean_base_rate(
    inputs: &TankVaporVentingInputs,
    ctx: &RunContext,
    hourly: &[HourlyTvv],
) -> Vec<WeightedMeanBaseRate> {
    // Shared join indexes.
    let source_bin_of: HashMap<i64, &SourceBinRow> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb))
        .collect();
    let evap_fuel_type: HashMap<i32, bool> = inputs
        .fuel_type
        .iter()
        .map(|ft| (ft.fuel_type_id, ft.subject_to_evap_calculations))
        .collect();
    let mut ppmy_by_pp_my: HashMap<(i32, i32), Vec<&PollutantProcessModelYearRow>> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        ppmy_by_pp_my
            .entry((ppmy.pol_process_id, ppmy.model_year_id))
            .or_default()
            .push(ppmy);
    }
    // A FuelType row with subjectToEvapCalculations = 'Y'.
    let evap_ok = |fuel_type_id: i32| evap_fuel_type.get(&fuel_type_id) == Some(&true);
    // A PollutantProcessModelYear row links the bin's model-year group.
    let ppmy_links = |pol_process_id: i32, model_year_id: i32, model_year_group_id: i32| {
        ppmy_by_pp_my
            .get(&(pol_process_id, model_year_id))
            .is_some_and(|rows| {
                rows.iter()
                    .any(|p| p.model_year_group_id == model_year_group_id)
            })
    };

    let mut grouped: HashMap<WeightedRateKey, (f64, f64)> = HashMap::new();

    // --- Cold-soak insert: opModeID = 151, from HourlyTVV ---
    let mut stmy_by_my_st: HashMap<(i32, i32), Vec<&SourceTypeModelYearRow>> = HashMap::new();
    for stmy in &inputs.source_type_model_year {
        stmy_by_my_st
            .entry((stmy.model_year_id, stmy.source_type_id))
            .or_default()
            .push(stmy);
    }
    let mut sbd_by_stmy_pp: HashMap<(i32, i32), Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        sbd_by_stmy_pp
            .entry((sbd.source_type_model_year_id, sbd.pol_process_id))
            .or_default()
            .push(sbd);
    }
    for htvv in hourly {
        let Some(stmys) = stmy_by_my_st.get(&(ctx.year - htvv.age_id, htvv.source_type_id)) else {
            continue;
        };
        for stmy in stmys {
            let Some(sbds) =
                sbd_by_stmy_pp.get(&(stmy.source_type_model_year_id, htvv.pol_process_id))
            else {
                continue;
            };
            for sbd in sbds {
                let Some(sb) = source_bin_of.get(&sbd.source_bin_id) else {
                    continue;
                };
                if sb.fuel_type_id != htvv.fuel_type_id || sb.reg_class_id != htvv.reg_class_id {
                    continue;
                }
                if !evap_ok(sb.fuel_type_id) {
                    continue;
                }
                if !ppmy_links(
                    sbd.pol_process_id,
                    stmy.model_year_id,
                    sb.model_year_group_id,
                ) {
                    continue;
                }
                let entry = grouped
                    .entry((
                        htvv.pol_process_id,
                        htvv.source_type_id,
                        sb.fuel_type_id,
                        htvv.month_id,
                        htvv.hour_day_id,
                        stmy.model_year_id,
                        151,
                    ))
                    .or_insert((0.0, 0.0));
                entry.0 += sbd.source_bin_activity_fraction * htvv.hourly_tvv;
                entry.1 += sbd.source_bin_activity_fraction * htvv.hourly_tvv_im;
            }
        }
    }

    // --- Operating / hot-soak insert: opModeID ∈ {150, 300}, from EmissionRateByAge ---
    let mut age_by_group: HashMap<i32, Vec<&AgeCategoryRow>> = HashMap::new();
    for ac in &inputs.age_category {
        age_by_group.entry(ac.age_group_id).or_default().push(ac);
    }
    let mut sbd_by_bin_pp: HashMap<(i64, i32), Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        sbd_by_bin_pp
            .entry((sbd.source_bin_id, sbd.pol_process_id))
            .or_default()
            .push(sbd);
    }
    let stmy_of: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();
    let run_spec_source_types: HashSet<i32> = inputs.run_spec_source_type.iter().copied().collect();

    for er in &inputs.emission_rate_by_age {
        let Some(sb) = source_bin_of.get(&er.source_bin_id) else {
            continue;
        };
        if !evap_ok(sb.fuel_type_id) {
            continue;
        }
        let Some(ages) = age_by_group.get(&er.age_group_id) else {
            continue;
        };
        let Some(sbds) = sbd_by_bin_pp.get(&(sb.source_bin_id, er.pol_process_id)) else {
            continue;
        };
        for ac in ages {
            for sbd in sbds {
                let Some(stmy) = stmy_of.get(&sbd.source_type_model_year_id) else {
                    continue;
                };
                if stmy.model_year_id != ctx.year - ac.age_id {
                    continue;
                }
                if !run_spec_source_types.contains(&stmy.source_type_id) {
                    continue;
                }
                if !ppmy_links(
                    sbd.pol_process_id,
                    stmy.model_year_id,
                    sb.model_year_group_id,
                ) {
                    continue;
                }
                let contribution = sbd.source_bin_activity_fraction * er.mean_base_rate;
                let contribution_im = sbd.source_bin_activity_fraction * er.mean_base_rate_im;
                // Cross join RunSpecMonth × RunSpecHourDay.
                for &month_id in &inputs.run_spec_month {
                    for &hour_day_id in &inputs.run_spec_hour_day {
                        let entry = grouped
                            .entry((
                                er.pol_process_id,
                                stmy.source_type_id,
                                sb.fuel_type_id,
                                month_id,
                                hour_day_id,
                                stmy.model_year_id,
                                er.op_mode_id,
                            ))
                            .or_insert((0.0, 0.0));
                        entry.0 += contribution;
                        entry.1 += contribution_im;
                    }
                }
            }
        }
    }

    let mut out: Vec<WeightedMeanBaseRate> = grouped
        .into_iter()
        .map(|(key, (rate, rate_im))| WeightedMeanBaseRate {
            pol_process_id: key.0,
            source_type_id: key.1,
            fuel_type_id: key.2,
            month_id: key.3,
            hour_day_id: key.4,
            model_year_id: key.5,
            op_mode_id: key.6,
            weighted_mean_base_rate: rate,
            weighted_mean_base_rate_im: rate_im,
        })
        .collect();
    out.sort_unstable_by_key(|w| {
        (
            w.pol_process_id,
            w.source_type_id,
            w.fuel_type_id,
            w.month_id,
            w.hour_day_id,
            w.model_year_id,
            w.op_mode_id,
        )
    });
    out
}

/// TVV-9 — assemble the `MOVESWorkerOutput` rows.
///
/// Each [`WeightedMeanBaseRate`] row is joined to `SourceHours` (on
/// `hourDayID`, `monthID`, the derived `ageID = year − modelYearID`, and
/// `sourceTypeID`), to `OpModeDistribution` (on `sourceTypeID`,
/// `hourDayID`, `polProcessID` and `opModeID`), and through
/// `PollutantProcessAssoc` and `HourDay`. The emission is
///
/// ```text
/// emissionQuant   = weightedMeanBaseRate   · sourceHours · opModeFraction
/// emissionQuantIM = weightedMeanBaseRateIM · sourceHours · opModeFraction
/// ```
///
/// The SQL's closing `UPDATE … set emissionQuant = GREATEST(emissionQuantIM
/// · IMAdjustFract + emissionQuant · (1 − IMAdjustFract), 0)` blends in the
/// I/M adjustment for every row matching an `IMCoverageMergedUngrouped`
/// entry on `(processID, pollutantID, modelYearID, fuelTypeID,
/// sourceTypeID)`; rows with no I/M coverage keep the plain `emissionQuant`.
/// The blend is folded into the row build here. `SourceHours`'s `yearID` /
/// `linkID` and `OpModeDistribution`'s `linkID` join conditions repeat
/// extract filters and are omitted (see the module scope notes).
fn assemble_emission_output(
    inputs: &TankVaporVentingInputs,
    ctx: &RunContext,
    weighted_rates: &[WeightedMeanBaseRate],
    im_merged: &[ImCoverageMerged],
) -> Vec<TankVaporVentingEmissionRow> {
    let mut source_hours_by_key: HashMap<(i32, i32, i32, i32), Vec<&SourceHoursRow>> =
        HashMap::new();
    for sh in &inputs.source_hours {
        source_hours_by_key
            .entry((sh.hour_day_id, sh.month_id, sh.age_id, sh.source_type_id))
            .or_default()
            .push(sh);
    }
    let mut omd_by_key: HashMap<(i32, i32, i32, i32), Vec<&OpModeDistributionRow>> = HashMap::new();
    for omd in &inputs.op_mode_distribution {
        omd_by_key
            .entry((
                omd.source_type_id,
                omd.hour_day_id,
                omd.pol_process_id,
                omd.op_mode_id,
            ))
            .or_default()
            .push(omd);
    }
    let assoc_of: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    // IMCoverageMergedUngrouped, unique on its five-column index.
    let im_adjust_of: HashMap<ImMergedKey, f64> = im_merged
        .iter()
        .map(|m| {
            (
                (
                    m.process_id,
                    m.pollutant_id,
                    m.model_year_id,
                    m.fuel_type_id,
                    m.source_type_id,
                ),
                m.im_adjust_fract,
            )
        })
        .collect();

    let mut out = Vec::new();
    for w in weighted_rates {
        let Some(source_hours) = source_hours_by_key.get(&(
            w.hour_day_id,
            w.month_id,
            ctx.year - w.model_year_id,
            w.source_type_id,
        )) else {
            continue;
        };
        for sh in source_hours {
            let Some(omds) = omd_by_key.get(&(
                sh.source_type_id,
                w.hour_day_id,
                w.pol_process_id,
                w.op_mode_id,
            )) else {
                continue;
            };
            for omd in omds {
                let Some(assoc) = assoc_of.get(&omd.pol_process_id) else {
                    continue;
                };
                let Some(hd) = hour_day_of.get(&omd.hour_day_id) else {
                    continue;
                };
                let emission_quant =
                    w.weighted_mean_base_rate * sh.source_hours * omd.op_mode_fraction;
                let emission_quant_im =
                    w.weighted_mean_base_rate_im * sh.source_hours * omd.op_mode_fraction;
                // Apply I/M: blend where an IMCoverageMergedUngrouped row matches.
                let final_quant = match im_adjust_of.get(&(
                    assoc.process_id,
                    assoc.pollutant_id,
                    w.model_year_id,
                    w.fuel_type_id,
                    w.source_type_id,
                )) {
                    Some(&im_adjust) => (emission_quant_im * im_adjust
                        + emission_quant * (1.0 - im_adjust))
                        .max(0.0),
                    None => emission_quant,
                };
                out.push(TankVaporVentingEmissionRow {
                    year_id: ctx.year,
                    month_id: w.month_id,
                    day_id: hd.day_id,
                    hour_id: hd.hour_id,
                    state_id: ctx.state_id,
                    county_id: ctx.county_id,
                    zone_id: ctx.zone_id,
                    link_id: ctx.link_id,
                    pollutant_id: assoc.pollutant_id,
                    process_id: assoc.process_id,
                    source_type_id: w.source_type_id,
                    fuel_type_id: w.fuel_type_id,
                    model_year_id: w.model_year_id,
                    road_type_id: ctx.road_type_id,
                    emission_quant: final_quant,
                });
            }
        }
    }
    out
}

/// The MOVES single-day tank vapor venting calculator.
///
/// A small value type owning no per-run state — only its single master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input
/// flows through the [`TankVaporVentingInputs`] / [`RunContext`] arguments
/// to [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct TankVaporVentingCalculator {
    /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl TankVaporVentingCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator with its master-loop subscription.
    ///
    /// The Java constructor signs up for the Evap Fuel Vapor Venting process
    /// (12) at `MONTH` granularity with the base `EMISSION_CALCULATOR`
    /// priority — its `0` offset carries the comment "no offset from the
    /// standard MasterLoopPriority.EMISSION_CALCULATOR". The
    /// `CalculatorInfo.txt` `Subscribe` directive records the same single
    /// subscription.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                ProcessId(EVAP_FUEL_VAPOR_VENTING_PROCESS_ID),
                Granularity::Month,
                priority,
            )],
        }
    }

    /// Compute the tank-vapor-venting emission rows — the port of the
    /// `TankVaporVentingCalculator.sql` "Processing" section.
    ///
    /// The nine numbered TVV steps run in order: TVV-1 merges the I/M
    /// adjustment fractions, TVV-2 finds the peak cold-soak hour, TVV-3
    /// generates tank vapor by ethanol level, TVV-4 ethanol-weights it,
    /// TVV-5 accumulates vapor vented, TVV-6 differences it into hourly
    /// increments, TVV-7 cold-soak-weights it and adds the post-peak decay
    /// hours, TVV-8 builds the weighted mean base rates, and TVV-9
    /// assembles the output. The result is sorted by its integer dimension
    /// columns (ties broken on `emissionQuant`) for deterministic output —
    /// MOVES leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(
        &self,
        inputs: &TankVaporVentingInputs,
        ctx: &RunContext,
    ) -> Vec<TankVaporVentingEmissionRow> {
        let im_merged = im_coverage_merged_ungrouped(inputs, ctx);
        let peak_hours = peak_hour_of_cold_soak(inputs);
        let generated = tank_vapor_generated(inputs, &peak_hours);
        let weighted = ethanol_weighted_tvg(inputs, &generated);
        let cumulative = cumulative_tank_vapor_vented(inputs, &weighted);
        let unweighted = unweighted_hourly_tvv(&cumulative, ctx.zone_id);
        let hourly = hourly_tvv(inputs, &unweighted, &peak_hours);
        let weighted_rates = weighted_mean_base_rate(inputs, ctx, &hourly);
        let mut output = assemble_emission_output(inputs, ctx, &weighted_rates, &im_merged);

        // MOVESWorkerOutput is physically unordered; sort for reproducibility.
        // Two output rows can share a dimension key — the SQL writes one row
        // per operating mode but does not carry opModeID into the output — so
        // ties break on emissionQuant for a fully canonical order.
        output.sort_by(|a, b| {
            a.dimension_key()
                .cmp(&b.dimension_key())
                .then_with(|| a.emission_quant.total_cmp(&b.emission_quant))
        });
        output
    }
}

impl Default for TankVaporVentingCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// The one `(pollutant, process)` pair the calculator registers — Total
/// Gaseous Hydrocarbons × Evap Fuel Vapor Venting.
///
/// Matches the single `Registration` directive recorded for
/// `TankVaporVentingCalculator` in `CalculatorInfo.txt`
/// (`registrations_count: 1` in `calculator-dag.json`).
static REGISTRATIONS: [PollutantProcessAssociation; 1] = [PollutantProcessAssociation {
    pollutant_id: PollutantId(TOTAL_HYDROCARBONS_POLLUTANT_ID),
    process_id: ProcessId(EVAP_FUEL_VAPOR_VENTING_PROCESS_ID),
}];

/// Default-DB / execution-DB tables the venting computation consumes — the
/// [`TankVaporVentingInputs`] fields. The SQL additionally extracts
/// `AverageTankTemperature`, `Link`, `RunSpecDay` and
/// `SourceTypeModelYearGroup`, which the single-day "Processing" section
/// never reads; they are omitted.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "AverageTankGasoline",
    "ColdSoakInitialHourFraction",
    "ColdSoakTankTemperature",
    "County",
    "CumTVVCoeffs",
    "EmissionRateByAge",
    "FuelType",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "MonthOfAnyYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessModelYear",
    "RunSpecHourDay",
    "RunSpecMonth",
    "RunSpecSourceType",
    "SourceBin",
    "SourceBinDistribution",
    "SourceHours",
    "SourceTypeModelYear",
    "TankVaporGenCoeffs",
    "Year",
    "Zone",
];

impl Calculator for TankVaporVentingCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    /// The one `(pollutant, process)` pair: THC × Evap Fuel Vapor Venting.
    /// See `REGISTRATIONS`.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): `calculator-dag.json`
    // records `depends_on: []`. `TankVaporVentingCalculator` subscribes
    // directly to the master loop; the `Chain` directive makes it an
    // upstream of `HCSpeciationCalculator`, not the reverse.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let run_ctx = RunContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            road_type_id: 0, // road_type_id not yet exposed in ExecutionLocation
        };
        let inputs = TankVaporVentingInputs {
            age_category: tables.iter_typed::<AgeCategoryRow>("AgeCategory")?,
            average_tank_gasoline: tables.iter_typed::<AverageTankGasolineRow>("AverageTankGasoline")?,
            cold_soak_initial_hour_fraction: tables.iter_typed::<ColdSoakInitialHourFractionRow>("ColdSoakInitialHourFraction")?,
            cold_soak_tank_temperature: tables.iter_typed::<ColdSoakTankTemperatureRow>("ColdSoakTankTemperature")?,
            county: tables.iter_typed::<CountyRow>("County")?,
            cum_tvv_coeffs: tables.iter_typed::<CumTvvCoeffsRow>("CumTVVCoeffs")?,
            emission_rate_by_age: tables.iter_typed::<EmissionRateByAgeRow>("EmissionRateByAge")?,
            fuel_type: tables.iter_typed::<FuelTypeRow>("FuelType")?,
            hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
            im_coverage: tables.iter_typed::<ImCoverageRow>("IMCoverage")?,
            im_factor: tables.iter_typed::<ImFactorRow>("IMFactor")?,
            month_of_any_year: tables.iter_typed::<MonthOfAnyYearRow>("MonthOfAnyYear")?,
            op_mode_distribution: tables.iter_typed::<OpModeDistributionRow>("OpModeDistribution")?,
            pollutant_process_assoc: tables.iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
            pollutant_process_model_year: tables.iter_typed::<PollutantProcessModelYearRow>("PollutantProcessModelYear")?,
            run_spec_hour_day: tables.iter_typed::<RunSpecHourDayIdRow>("RunSpecHourDay")?.into_iter().map(|r| r.hour_day_id).collect(),
            run_spec_month: tables.iter_typed::<RunSpecMonthIdRow>("RunSpecMonth")?.into_iter().map(|r| r.month_id).collect(),
            run_spec_source_type: tables.iter_typed::<RunSpecSourceTypeIdRow>("RunSpecSourceType")?.into_iter().map(|r| r.source_type_id).collect(),
            source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
            source_bin_distribution: tables.iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
            source_hours: tables.iter_typed::<SourceHoursRow>("SourceHours")?,
            source_type_model_year: tables.iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
            tank_vapor_gen_coeffs: tables.iter_typed::<TankVaporGenCoeffsRow>("TankVaporGenCoeffs")?,
            year: tables.iter_typed::<YearRow>("Year")?,
            zone: tables.iter_typed::<ZoneRow>("Zone")?,
        };
        let rows = self.calculate(&inputs, &run_ctx);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(TankVaporVentingCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run context the fixtures use: calendar year 2020, county 26161
    /// of state 26, zone 90, link 5001, road type 5.
    fn run_context() -> RunContext {
        RunContext {
            year: 2020,
            state_id: 26,
            county_id: 26_161,
            zone_id: 90,
            link_id: 5001,
            road_type_id: 5,
        }
    }

    /// Run the calculator over `inputs` with the standard [`run_context`].
    fn run(inputs: &TankVaporVentingInputs) -> Vec<TankVaporVentingEmissionRow> {
        TankVaporVentingCalculator::new().calculate(inputs, &run_context())
    }

    /// Assert two `emissionQuant`s match within `f64` slack.
    fn assert_quant(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "emissionQuant {actual} != expected {expected}",
        );
    }

    /// `D` — the temperature term `exp(0.01·70) − exp(0.01·60)` the
    /// minimal fixture's TVG reduces to (its `tvgTermB` is zero, so the RVP
    /// exponential is `exp(0) = 1`).
    fn temperature_term() -> f64 {
        (0.01_f64 * 70.0).exp() - (0.01_f64 * 60.0).exp()
    }

    /// A minimal one-of-everything input that threads exactly one row
    /// through all nine TVV steps via the cold-soak (`opModeID = 151`) path.
    ///
    /// Hand-computed: TVG (both ethanol levels, identical coefficients) is
    /// `1 · exp(0·9) · (exp(0.7) − exp(0.6)) = D`; TVV-4 with `ETOHVolume`
    /// 10 weights entirely to level 10, so `ethanolWeightedTVG = D`; TVV-5
    /// `tankVaporVented = max(0 + D·(1 + 0·D), 0) = D`; TVV-6 has no prior
    /// hour so `unweightedHourlyTVV = D`; TVV-7 weights by the cold-soak
    /// fraction 1.0; TVV-8 weights by `sourceBinActivityFraction` 1.0; TVV-9
    /// `emissionQuant = D · sourceHours(10) · opModeFraction(1) = 10·D`.
    fn minimal_inputs() -> TankVaporVentingInputs {
        TankVaporVentingInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 300,
            }],
            average_tank_gasoline: vec![AverageTankGasolineRow {
                fuel_type_id: 1,
                fuel_year_id: 2020,
                month_group_id: 7,
                etoh_volume: 10.0,
                rvp: 9.0,
            }],
            cold_soak_initial_hour_fraction: vec![ColdSoakInitialHourFractionRow {
                source_type_id: 21,
                zone_id: 90,
                month_id: 7,
                hour_day_id: 25,
                initial_hour_day_id: 15,
                cold_soak_initial_hour_fraction: 1.0,
            }],
            cold_soak_tank_temperature: vec![
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 1,
                    cold_soak_tank_temperature: 60.0,
                },
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 2,
                    cold_soak_tank_temperature: 70.0,
                },
            ],
            county: vec![CountyRow {
                county_id: 26_161,
                altitude: 'L',
            }],
            cum_tvv_coeffs: vec![CumTvvCoeffsRow {
                reg_class_id: 30,
                model_year_group_id: 0,
                age_group_id: 300,
                pol_process_id: 112,
                tvv_term_a: 0.0,
                tvv_term_b: 1.0,
                tvv_term_c: 0.0,
                tvv_term_a_im: 0.0,
                tvv_term_b_im: 1.0,
                tvv_term_c_im: 0.0,
            }],
            emission_rate_by_age: Vec::new(),
            fuel_type: vec![FuelTypeRow {
                fuel_type_id: 1,
                subject_to_evap_calculations: true,
            }],
            hour_day: vec![
                HourDayRow {
                    hour_day_id: 15,
                    day_id: 5,
                    hour_id: 1,
                },
                HourDayRow {
                    hour_day_id: 25,
                    day_id: 5,
                    hour_id: 2,
                },
            ],
            im_coverage: Vec::new(),
            im_factor: Vec::new(),
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 7,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 25,
                link_id: 5001,
                pol_process_id: 112,
                op_mode_id: 151,
                op_mode_fraction: 1.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 112,
                process_id: 12,
                pollutant_id: 1,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 112,
                model_year_id: 2018,
                model_year_group_id: 0,
                im_model_year_group_id: 0,
            }],
            run_spec_hour_day: vec![15, 25],
            run_spec_month: vec![7],
            run_spec_source_type: vec![21],
            source_bin: vec![SourceBinRow {
                source_bin_id: 500_000,
                fuel_type_id: 1,
                reg_class_id: 30,
                model_year_group_id: 0,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 2118,
                pol_process_id: 112,
                source_bin_id: 500_000,
                source_bin_activity_fraction: 1.0,
            }],
            source_hours: vec![SourceHoursRow {
                hour_day_id: 25,
                month_id: 7,
                age_id: 2,
                source_type_id: 21,
                source_hours: 10.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 2118,
                model_year_id: 2018,
                source_type_id: 21,
            }],
            tank_vapor_gen_coeffs: vec![
                TankVaporGenCoeffsRow {
                    ethanol_level_id: 0,
                    altitude: 'L',
                    tvg_term_a: 1.0,
                    tvg_term_b: 0.0,
                    tvg_term_c: 0.01,
                },
                TankVaporGenCoeffsRow {
                    ethanol_level_id: 10,
                    altitude: 'L',
                    tvg_term_a: 1.0,
                    tvg_term_b: 0.0,
                    tvg_term_c: 0.01,
                },
            ],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            zone: vec![ZoneRow {
                zone_id: 90,
                county_id: 26_161,
            }],
        }
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = run(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5); // from HourDay
        assert_eq!(r.hour_id, 2); // from HourDay
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 90);
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.pollutant_id, 1); // from PollutantProcessAssoc
        assert_eq!(r.process_id, 12); // from PollutantProcessAssoc
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.fuel_type_id, 1);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 5);
        assert_quant(r.emission_quant, 10.0 * temperature_term());
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(run(&TankVaporVentingInputs::default()).is_empty());
    }

    #[test]
    fn calculate_blends_in_the_im_adjustment() {
        // An I/M program covering model year 2018: IMFactor 1.0 ×
        // complianceFactor 50 × 0.01 = 0.5 adjustment fraction. The I/M
        // coefficient `tvvTermBIM = 2.0` doubles the I/M emission, so the
        // blend is 20D·0.5 + 10D·0.5 = 15D.
        let mut inputs = minimal_inputs();
        inputs.cum_tvv_coeffs[0].tvv_term_b_im = 2.0;
        inputs.im_factor = vec![ImFactorRow {
            pol_process_id: 112,
            inspect_freq: 1,
            test_standards_id: 1,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 0,
            age_group_id: 300,
            im_factor: 1.0,
        }];
        inputs.im_coverage = vec![ImCoverageRow {
            pol_process_id: 112,
            inspect_freq: 1,
            test_standards_id: 1,
            source_type_id: 21,
            fuel_type_id: 1,
            beg_model_year_id: 2000,
            end_model_year_id: 2050,
            compliance_factor: 50.0,
        }];
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 15.0 * temperature_term());
    }

    #[test]
    fn calculate_interpolates_tvg_between_ethanol_levels() {
        // Distinct level-0 and level-10 vapor generation; ETOHVolume 5.0
        // puts the ethanol weight at least(10, 5)/10 = 0.5, so
        // ethanolWeightedTVG = 3D·0.5 + D·0.5 = 2D and emissionQuant = 20D.
        let mut inputs = minimal_inputs();
        inputs.tank_vapor_gen_coeffs[1].tvg_term_a = 3.0;
        inputs.average_tank_gasoline[0].etoh_volume = 5.0;
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 20.0 * temperature_term());
    }

    #[test]
    fn calculate_clamps_negative_cumulative_tvv_to_zero() {
        // A large negative tvvTermA drives tankVaporVented below zero; the
        // SQL `greatest(…, 0)` floors it, so the emission is exactly zero.
        let mut inputs = minimal_inputs();
        inputs.cum_tvv_coeffs[0].tvv_term_a = -100.0;
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].emission_quant, 0.0);
    }

    #[test]
    fn calculate_handles_the_operating_mode_emission_rate_path() {
        // An EmissionRateByAge row drives the opModeID-300 insert, which
        // bypasses the TVG chain: weightedMeanBaseRate = 4.0.
        let mut inputs = minimal_inputs();
        inputs.emission_rate_by_age = vec![EmissionRateByAgeRow {
            source_bin_id: 500_000,
            pol_process_id: 112,
            op_mode_id: 300,
            age_group_id: 300,
            mean_base_rate: 4.0,
            mean_base_rate_im: 4.0,
        }];
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            source_type_id: 21,
            hour_day_id: 25,
            link_id: 5001,
            pol_process_id: 112,
            op_mode_id: 300,
            op_mode_fraction: 1.0,
        });
        let rows = run(&inputs);
        // The opMode-151 venting row plus the opMode-300 operating row.
        assert_eq!(rows.len(), 2);
        // opMode-300: weightedMeanBaseRate 4.0 × sourceHours 10 × fraction 1.
        assert!(
            rows.iter().any(|r| (r.emission_quant - 40.0).abs() < 1e-9),
            "missing the operating-mode emission row",
        );
    }

    #[test]
    fn calculate_skips_fuel_type_not_subject_to_evap() {
        // FuelType.subjectToEvapCalculations = 'N' drops every TVV-8 row.
        let mut inputs = minimal_inputs();
        inputs.fuel_type[0].subject_to_evap_calculations = false;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_row_without_op_mode_distribution() {
        // TVV-9 inner-joins OpModeDistribution; with none, no output.
        let mut inputs = minimal_inputs();
        inputs.op_mode_distribution.clear();
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key_then_quant() {
        // The operating-mode fixture yields two rows sharing a dimension
        // key (opModeID is not an output column); they come back ordered.
        let mut inputs = minimal_inputs();
        inputs.emission_rate_by_age = vec![EmissionRateByAgeRow {
            source_bin_id: 500_000,
            pol_process_id: 112,
            op_mode_id: 300,
            age_group_id: 300,
            mean_base_rate: 4.0,
            mean_base_rate_im: 4.0,
        }];
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            source_type_id: 21,
            hour_day_id: 25,
            link_id: 5001,
            pol_process_id: 112,
            op_mode_id: 300,
            op_mode_fraction: 1.0,
        });
        let rows = run(&inputs);
        assert!(rows.len() >= 2);
        assert!(
            rows.windows(2).all(|w| {
                w[0].dimension_key() < w[1].dimension_key()
                    || (w[0].dimension_key() == w[1].dimension_key()
                        && w[0].emission_quant <= w[1].emission_quant)
            }),
            "calculate output is not sorted",
        );
    }

    #[test]
    fn peak_hour_of_cold_soak_takes_the_warmest_hour_breaking_ties_low() {
        let inputs = TankVaporVentingInputs {
            cold_soak_tank_temperature: vec![
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 1,
                    cold_soak_tank_temperature: 68.0,
                },
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 2,
                    cold_soak_tank_temperature: 70.0,
                },
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 3,
                    cold_soak_tank_temperature: 70.0,
                },
                ColdSoakTankTemperatureRow {
                    month_id: 7,
                    hour_id: 4,
                    cold_soak_tank_temperature: 69.0,
                },
            ],
            ..TankVaporVentingInputs::default()
        };
        let peaks = peak_hour_of_cold_soak(&inputs);
        assert_eq!(peaks.len(), 1);
        assert_eq!(peaks[0].month_id, 7);
        // Hours 2 and 3 tie at 70.0 °F; the earlier hour wins.
        assert_eq!(peaks[0].peak_hour_id, 2);
    }

    #[test]
    fn unweighted_hourly_tvv_differences_the_prior_hour() {
        let row = |hour_id, prior_hour_id, tvv| CumulativeTankVaporVented {
            reg_class_id: 30,
            age_id: 2,
            pol_process_id: 112,
            day_id: 5,
            hour_id,
            initial_hour_day_id: 15,
            month_id: 7,
            source_type_id: 21,
            fuel_type_id: 1,
            tank_vapor_vented: tvv,
            tank_vapor_vented_im: tvv,
            hour_day_id: hour_id * 10 + 5,
            prior_hour_id,
        };
        // Hour 1 → cumulative 3.0; hour 2 → cumulative 5.0, prior hour 1.
        let cumulative = vec![row(1, 24, 3.0), row(2, 1, 5.0)];
        let out = unweighted_hourly_tvv(&cumulative, 90);
        assert_eq!(out.len(), 2);
        // Hour 1 has no prior row: unweighted = cumulative = 3.0.
        let h1 = out.iter().find(|u| u.hour_day_id == 15).unwrap();
        assert_eq!(h1.unweighted_hourly_tvv, 3.0);
        assert_eq!(h1.zone_id, 90);
        // Hour 2's prior is hour 1: unweighted = 5.0 − 3.0 = 2.0.
        let h2 = out.iter().find(|u| u.hour_day_id == 25).unwrap();
        assert_eq!(h2.unweighted_hourly_tvv, 2.0);
    }

    #[test]
    fn hourly_tvv_emits_the_post_peak_decay_hours() {
        let inputs = TankVaporVentingInputs {
            hour_day: vec![
                HourDayRow {
                    hour_day_id: 25,
                    day_id: 5,
                    hour_id: 2,
                },
                HourDayRow {
                    hour_day_id: 35,
                    day_id: 5,
                    hour_id: 3,
                },
                HourDayRow {
                    hour_day_id: 45,
                    day_id: 5,
                    hour_id: 4,
                },
                HourDayRow {
                    hour_day_id: 55,
                    day_id: 5,
                    hour_id: 5,
                },
                HourDayRow {
                    hour_day_id: 65,
                    day_id: 5,
                    hour_id: 6,
                },
            ],
            cold_soak_initial_hour_fraction: vec![ColdSoakInitialHourFractionRow {
                source_type_id: 21,
                zone_id: 90,
                month_id: 7,
                hour_day_id: 25,
                initial_hour_day_id: 15,
                cold_soak_initial_hour_fraction: 1.0,
            }],
            ..TankVaporVentingInputs::default()
        };
        let unweighted = vec![UnweightedHourlyTvv {
            zone_id: 90,
            reg_class_id: 30,
            age_id: 2,
            pol_process_id: 112,
            hour_day_id: 25,
            initial_hour_day_id: 15,
            month_id: 7,
            source_type_id: 21,
            fuel_type_id: 1,
            unweighted_hourly_tvv: 100.0,
            unweighted_hourly_tvv_im: 100.0,
        }];
        let peak = vec![PeakHourOfColdSoak {
            month_id: 7,
            peak_hour_id: 2,
        }];
        let out = hourly_tvv(&inputs, &unweighted, &peak);
        // Part A: the peak-hour row (hourDay 25) = 100.0 × fraction 1.0.
        let at_peak = out.iter().find(|h| h.hour_day_id == 25).unwrap();
        assert_eq!(at_peak.hourly_tvv, 100.0);
        // Part B: four decay hours scaled 0.0200 / 0.0100 / 0.0040 / 0.0005.
        let decay = |hour_day_id| out.iter().find(|h| h.hour_day_id == hour_day_id).unwrap();
        assert_quant(decay(35).hourly_tvv, 2.0);
        assert_quant(decay(45).hourly_tvv, 1.0);
        assert_quant(decay(55).hourly_tvv, 0.4);
        assert_quant(decay(65).hourly_tvv, 0.05);
        assert_eq!(out.len(), 5);
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(
            TankVaporVentingCalculator::new().name(),
            "TankVaporVentingCalculator",
        );
        assert_eq!(
            TankVaporVentingCalculator::NAME,
            "TankVaporVentingCalculator",
        );
    }

    #[test]
    fn calculator_subscribes_to_evap_fuel_vapor_venting_at_month() {
        let calc = TankVaporVentingCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(12)); // Evap Fuel Vapor Venting
        assert_eq!(subs[0].granularity, Granularity::Month);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");
    }

    #[test]
    fn calculator_registers_thc_evap_fuel_vapor_venting() {
        // One Registration directive: pollutant 1 (THC) × process 12.
        let calc = TankVaporVentingCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 1);
        assert_eq!(regs[0].pollutant_id, PollutantId(1));
        assert_eq!(regs[0].process_id, ProcessId(12));
    }

    #[test]
    fn calculator_declares_input_tables_and_no_upstream() {
        let calc = TankVaporVentingCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "CumTVVCoeffs",
            "ColdSoakTankTemperature",
            "IMCoverage",
            "TankVaporGenCoeffs",
            "SourceBinDistribution",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
        // `calculator-dag.json` records `depends_on: []`.
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        // Use store.insert(...) directly to bypass schema-registry validation:
        // the TVV-specific column subsets differ from the registry canonical schemas.
        store.insert("AgeCategory", AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap());
        store.insert("AverageTankGasoline", AverageTankGasolineRow::into_dataframe(inputs.average_tank_gasoline.clone()).unwrap());
        store.insert("ColdSoakInitialHourFraction", ColdSoakInitialHourFractionRow::into_dataframe(inputs.cold_soak_initial_hour_fraction.clone()).unwrap());
        store.insert("ColdSoakTankTemperature", ColdSoakTankTemperatureRow::into_dataframe(inputs.cold_soak_tank_temperature.clone()).unwrap());
        store.insert("County", CountyRow::into_dataframe(inputs.county.clone()).unwrap());
        store.insert("CumTVVCoeffs", CumTvvCoeffsRow::into_dataframe(inputs.cum_tvv_coeffs.clone()).unwrap());
        store.insert("EmissionRateByAge", EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age.clone()).unwrap());
        store.insert("FuelType", FuelTypeRow::into_dataframe(inputs.fuel_type.clone()).unwrap());
        store.insert("HourDay", HourDayRow::into_dataframe(inputs.hour_day.clone()).unwrap());
        store.insert("IMCoverage", ImCoverageRow::into_dataframe(inputs.im_coverage.clone()).unwrap());
        store.insert("IMFactor", ImFactorRow::into_dataframe(inputs.im_factor.clone()).unwrap());
        store.insert("MonthOfAnyYear", MonthOfAnyYearRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap());
        store.insert("OpModeDistribution", OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution.clone()).unwrap());
        store.insert("PollutantProcessAssoc", PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc.clone()).unwrap());
        store.insert("PollutantProcessModelYear", PollutantProcessModelYearRow::into_dataframe(inputs.pollutant_process_model_year.clone()).unwrap());
        // RunSpec* tables use thin wrapper types
        store.insert("RunSpecHourDay", RunSpecHourDayIdRow::into_dataframe(
            inputs.run_spec_hour_day.iter().map(|&id| RunSpecHourDayIdRow { hour_day_id: id }).collect()
        ).unwrap());
        store.insert("RunSpecMonth", RunSpecMonthIdRow::into_dataframe(
            inputs.run_spec_month.iter().map(|&id| RunSpecMonthIdRow { month_id: id }).collect()
        ).unwrap());
        store.insert("RunSpecSourceType", RunSpecSourceTypeIdRow::into_dataframe(
            inputs.run_spec_source_type.iter().map(|&id| RunSpecSourceTypeIdRow { source_type_id: id }).collect()
        ).unwrap());
        store.insert("SourceBin", SourceBinRow::into_dataframe(inputs.source_bin.clone()).unwrap());
        store.insert("SourceBinDistribution", SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution.clone()).unwrap());
        store.insert("SourceHours", SourceHoursRow::into_dataframe(inputs.source_hours.clone()).unwrap());
        store.insert("SourceTypeModelYear", SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap());
        store.insert("TankVaporGenCoeffs", TankVaporGenCoeffsRow::into_dataframe(inputs.tank_vapor_gen_coeffs.clone()).unwrap());
        store.insert("Year", YearRow::into_dataframe(inputs.year.clone()).unwrap());
        store.insert("Zone", ZoneRow::into_dataframe(inputs.zone.clone()).unwrap());

        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = TankVaporVentingCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(out.dataframe().unwrap().height() > 0, "expected at least one row");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "TankVaporVentingCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calc: Box<dyn Calculator> = Box::new(TankVaporVentingCalculator::new());
        assert_eq!(calc.name(), "TankVaporVentingCalculator");
        assert_eq!(calc.registrations().len(), 1);
    }
}
