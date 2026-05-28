//! Port of `database/MultidayTankVaporVentingCalculator.sql` — migration
//! plan Phase 3, Task 60.
//!
//! `MultidayTankVaporVentingCalculator.sql` is the **multi-day diurnal**
//! tank-vapor-venting script — at 2,066 lines the largest single calculator
//! SQL file in MOVES. It computes the same emission as the single-day
//! `TankVaporVentingCalculator.sql` (migration-plan Task 59) — the **tank
//! vapor venting** of total gaseous hydrocarbons (THC, pollutant 1) vented
//! from a vehicle's fuel tank under the Evap Fuel Vapor Venting process (12)
//! — but tracks a vehicle's tank vapour across **multiple successive soaking
//! days** rather than a single diurnal cycle.
//!
//! # Not a distinct calculator
//!
//! There is no `MultidayTankVaporVentingCalculator` Java class and no such
//! entry in `CalculatorInfo.txt` or `calculator-dag.json`: the single Java
//! class `TankVaporVentingCalculator` selects *one* of two SQL scripts by
//! the `USE_MULTIDAY_DIURNALS` compilation flag —
//! `MultidayTankVaporVentingCalculator.sql` (this port) when the flag is set,
//! `TankVaporVentingCalculator.sql` (Task 59) otherwise. The pinned MOVES
//! build compiles with the flag **set**, so this multi-day script is the one
//! that actually runs; the single-day script carries an `@notused`
//! annotation.
//!
//! Because the *calculator* is `TankVaporVentingCalculator`, the live
//! `Registration` (THC × process 12) and `Subscribe` (process 12 at `MONTH`)
//! directives belong to the single-day module
//! [`crate::calculators::tank_vapor_venting_calculator`]. To avoid
//! double-registering that one pair, this module's
//! [`Calculator::registrations`] and [`Calculator::subscriptions`] return
//! **empty slices** — see their doc comments. This module is the multi-day
//! *algorithm body*; a future runtime would dispatch to it or to the
//! single-day port according to `USE_MULTIDAY_DIURNALS`.
//!
//! # Algorithm
//!
//! [`MultidayTankVaporVentingCalculator::calculate`] ports the SQL's
//! "Processing" section. Like the single-day script it runs nine numbered
//! `TVV-*` steps, but the multi-day script inserts a tank-vapour-generated
//! (`TVG`) soak-day recurrence between TVV-4 and TVV-5:
//!
//! | SQL step | SQL working table | This port |
//! |----------|-------------------|-----------|
//! | TVV-1 | `IMCoverageMergedUngrouped` | `im_coverage_merged_ungrouped` |
//! | TVV-2 | `PeakHourOfColdSoak` | `peak_hour_of_cold_soak` |
//! | TVV-3 | `TankVaporGenerated` | `tank_vapor_generated` |
//! | TVV-4 | `EthanolWeightedTVG` | `ethanol_weighted_tvg` |
//! | TVG | `tvgSum*` / `TVG` | `tvg_soak_recurrence` |
//! | TVV-5 | `CummulativeTankVaporVented` | `cumulative_tank_vapor_vented` |
//! | TVV-6 | `UnweightedHourlyTVV` | `unweighted_hourly_tvv` |
//! | TVV-7 | `HourlyTVV` | `hourly_tvv` |
//! | TVV-8 | `WeightedMeanBaseRate` | `weighted_mean_base_rate` |
//! | TVV-9 | `MOVESWorkerOutput` | `assemble_emission_output` |
//!
//! What the multi-day script adds over the single-day one:
//!
//! * **TVV-3** computes tank vapour generated for the *high* and *low*
//!   altitude coefficient sets and **interpolates by the county's barometric
//!   pressure**, where the single-day script picked one set by the county's
//!   altitude bucket. It also scales by `tankSize · (1 − tankFillFraction)`
//!   and carries `modelYearID` / `polProcessID` and the canister / leak
//!   coefficients from `stmyTVVCoeffs`.
//! * **TVV-4** additionally differences the (cumulative) ethanol-weighted TVG
//!   into an **hourly increment**.
//! * The **TVG soak recurrence** (`tvg_soak_recurrence`) accumulates a
//!   running canister load `Xn` across soaking days 1, 2, 3…: each day's
//!   carry-over is back-purged and capped at the canister capacity.
//! * **TVV-5** drives the cumulative TVV from **data-driven `tvvEquation` /
//!   `leakEquation` expressions** (see [`VentingEquations`]) and blends a
//!   leaking-canister term, weighting by the regulatory-class fraction.
//! * **TVV-6/7** carry a `soakDayID` dimension; TVV-7 first folds the
//!   multi-day soak fractions from `sampleVehicleSoaking` into
//!   `coldSoakInitialHourFraction`.
//! * **TVV-8** additionally applies a **temperature / RVP adjustment** to the
//!   operating (300) and hot-soak (150) modes and carries a `regClassID`
//!   dimension.
//!
//! # Data-driven equations
//!
//! TVV-5's `##tvvEquations##` / `##leakEquations##` placeholders are filled
//! by `TankVaporVentingCalculator.alterReplacementsAndSections`: it reads the
//! *distinct* `tvvEquation` / `leakEquation` strings out of `cumTVVCoeffs`
//! and builds a `CASE tvvEquation WHEN … THEN <expr> … END` whose `<expr>`s
//! are arbitrary SQL the MOVES default database supplies as data. The Java
//! treats those strings opaquely; the port does the same — it carries the
//! equation name on each [`StmyTvvEquationsRow`] and defers evaluation to a
//! caller-supplied [`VentingEquations`], passing the joined row's measures
//! in an [`EquationVars`]. The default-database equation set is not in the
//! source pin, so the port cannot inline it; this faithfully mirrors the
//! Java, which never knows the equations either.
//!
//! # Scope
//!
//! [`calculate`](MultidayTankVaporVentingCalculator::calculate) is the SQL
//! "Processing" section. Two earlier sections do real computation but are
//! **conditionally-enabled setup**, not the per-bundle venting algorithm,
//! and are scoped out exactly as the Task 59 port scoped out "Extract Data":
//!
//! * **`NewTVVYear`** (`enabledSectionNames.add("NewTVVYear")`, once per new
//!   calendar year) aggregates `CumTVVCoeffs` by the sample-vehicle
//!   regulatory-class fractions into `stmyTVVCoeffs` / `stmyTVVEquations`.
//!   This port takes those two tables as inputs.
//! * **`FillSampleVehicleSoaking`** (`enabledSectionNames.add(…)`, first
//!   bundle only, only when `sampleVehicleSoaking` is empty) derives the
//!   five-soak-day `sampleVehicleSoaking` fractions from the sample-vehicle
//!   trip tables. This port takes `sampleVehicleSoaking` as an input.
//!
//! A [`MultidayTankVaporVentingInputs`] *is* the post-extract,
//! post-`NewTVVYear`, post-`FillSampleVehicleSoaking` tables, so the port
//! does not re-apply the extract `WHERE` clauses (`zoneID`, `monthID`,
//! `yearID`, `countyID`, `linkID`, `polProcessID`, `opModeID`, model-year
//! range). Where the Processing section repeats an extract filter the port
//! omits it as redundant. The `-- Section Debug` blocks
//! (`DebugTankVaporGenerated`, `DebugTVVMOVESWorkerOutput`) build debug-only
//! tables that feed no output and are not ported. The
//! `WithRegClassID` / `NoRegClassID` toggle resolves to `WithRegClassID`
//! (`BundleUtilities` force-enables it); the port implements that variant
//! only, per the established scripted-calculator precedent.
//!
//! # Fidelity notes
//!
//! The SQL stores every working-table measure in a `FLOAT` (32-bit) column
//! while MariaDB evaluates the arithmetic in `DOUBLE`. This port computes in
//! `f64` end to end, so it does not reproduce the `f32` truncation MOVES
//! applies between steps — a sub-`1e-7` relative drift left to the
//! calculator-integration-validation task (`mo-fvuf`). The schema-nullable
//! `double` canister / leak / coefficient columns are model data populated
//! for every processed row and are modelled as `f64`. TVV-2's peak-hour
//! pack/unpack idiom is computed in exact integer arithmetic (see
//! `peak_hour_of_cold_soak`). The barometric-pressure interpolation uses
//! the SQL's literal endpoints (`29.069`, `24.087`) verbatim — there are no
//! integer/integer literal divisions, so the MariaDB
//! `div_precision_increment` rounding gotcha does not arise.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose execution
//! tables are Phase 2 placeholders until the `DataFrameStore` lands
//! (migration-plan Task 50), so `execute` cannot yet read the inputs nor
//! emit `MOVESWorkerOutput`. The numeric algorithm is fully ported and
//! unit-tested on [`calculate`](MultidayTankVaporVentingCalculator::calculate);
//! `execute` is a documented shell returning an empty [`CalculatorOutput`].

use std::collections::{HashMap, HashSet};

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — the SQL script `MultidayTankVaporVentingCalculator`
/// drives. Not an entry in `calculator-dag.json` (see the module docs); used
/// only as this module's [`Calculator::name`].
const CALCULATOR_NAME: &str = "MultidayTankVaporVentingCalculator";

/// Cold-soak operating mode — `OpMode` 151. TVV-8's venting insert stamps it.
const COLD_SOAK_OP_MODE_ID: i32 = 151;

/// Operating (running) mode — `OpMode` 300. TVV-8 applies the temperature /
/// RVP adjustment to this mode only; the hot-soak mode 150 (the other mode
/// `EmissionRateByAge` is extracted to) takes the unit adjustment.
const OPERATING_OP_MODE_ID: i32 = 300;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables the SQL produces by its
// "Extract Data", "NewTVVYear" and "FillSampleVehicleSoaking" sections.
// Following the Phase 3 convention every `INT`/`SMALLINT` identifier is an
// `i32`, `sourceBinID` (`BIGINT`) is an `i64`, and every `FLOAT`/`DOUBLE`
// quantity is an `f64`. Only the columns the venting algorithm reads are
// modelled.
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
/// for a month group.
///
/// `AverageTankGasoline` is extracted to the run's single zone and year, so
/// the venting algorithm joins it on `monthGroupID` (and `fuelTypeID`) only;
/// `zoneID` and `fuelYearID` are not modelled. The `adjustTerm*` columns the
/// SQL `ALTER`s onto the table are computed in TVV-8 (see
/// `rvp_adjustment_terms`) and are not inputs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageTankGasolineRow {
    /// `fuelTypeID` — the fuel type.
    pub fuel_type_id: i32,
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
///
/// The extracted table has an implicit `soakDayID = 1` (the SQL `ALTER`s the
/// column on with `default 1`); TVV-7 adds the `soakDayID > 1` rows from
/// `sampleVehicleSoaking` (see `hourly_tvv`). Input rows are all
/// `soakDayID = 1`, so `soakDayID` is not a field here.
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
/// Extracted to the run's single zone, so the algorithm joins it on
/// `(monthID, hourID)` only and `zoneID` is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdSoakTankTemperatureRow {
    /// `monthID`.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `coldSoakTankTemperature` — the cold-soak tank temperature (°F).
    pub cold_soak_tank_temperature: f64,
}

/// One `County` row — supplies the barometric pressure TVV-3 interpolates by.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID` — the county primary key.
    pub county_id: i32,
    /// `barometricPressure` — the county barometric pressure; TVV-3
    /// interpolates the high- and low-altitude tank vapour generated by it.
    pub barometric_pressure: f64,
}

/// One `EmissionRateByAge` row — a source bin's mean base rate for an
/// age group and operating mode.
///
/// `EmissionRateByAge` is extracted to operating modes 150 and 300; TVV-8's
/// `opModeID IN (150,300)` filter is that extract repeated and the port omits
/// it. `opModeID` is carried because TVV-8 writes it onto
/// `WeightedMeanBaseRate`.
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

/// One `evapTemperatureAdjustment` row — the cubic temperature-adjustment
/// polynomial coefficients for the operating (300) mode.
///
/// Extracted to `processID = 12`; the SQL cross-joins the (single) extracted
/// row into TVV-8, so `processID` is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EvapTemperatureAdjustmentRow {
    /// `tempAdjustTerm3` — the cubic coefficient.
    pub temp_adjust_term_3: f64,
    /// `tempAdjustTerm2` — the quadratic coefficient.
    pub temp_adjust_term_2: f64,
    /// `tempAdjustTerm1` — the linear coefficient.
    pub temp_adjust_term_1: f64,
    /// `tempAdjustConstant` — the constant term.
    pub temp_adjust_constant: f64,
}

/// One `evapRVPTemperatureAdjustment` row — the cubic RVP-adjustment
/// polynomial coefficients at one RVP knot, for one fuel type.
///
/// Extracted to `processID = 12` and `fuelTypeID IN (1, 5)`; `processID` is
/// not modelled. TVV-8 linearly interpolates these by
/// [`AverageTankGasolineRow::rvp`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EvapRvpTemperatureAdjustmentRow {
    /// `fuelTypeID` — the fuel type the knot applies to.
    pub fuel_type_id: i32,
    /// `RVP` — the Reid-vapor-pressure knot.
    pub rvp: f64,
    /// `adjustTerm3` — the cubic coefficient.
    pub adjust_term_3: f64,
    /// `adjustTerm2` — the quadratic coefficient.
    pub adjust_term_2: f64,
    /// `adjustTerm1` — the linear coefficient.
    pub adjust_term_1: f64,
    /// `adjustConstant` — the constant term.
    pub adjust_constant: f64,
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
/// filters repeated and the port omits them.
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

/// One `sampleVehicleSoaking` row — the fraction of a source type's vehicles
/// still soaking after a given number of soaking days, at a given hour.
///
/// The `FillSampleVehicleSoaking` setup section derives this from the
/// sample-vehicle trip tables; this port takes it as an input. TVV-7 reads
/// the `soakDayID > 1` rows; the TVG soak recurrence
/// (`tvg_soak_recurrence`) reads the distinct `soakDayID`s to know how many
/// soaking days to iterate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleVehicleSoakingRow {
    /// `soakDayID` — the soaking-day index (0 = hourly basis, 1, 2, …).
    pub soak_day_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `dayID` — day-of-week type.
    pub day_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `soakFraction` — the fraction of vehicles still soaking.
    pub soak_fraction: f64,
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

/// One `SourceHours` row — per `(hourDay, month, age, sourceType)` source
/// operating hours.
///
/// `SourceHours` is extracted to the run's month, year and link; TVV-9's
/// `yearID` / `linkID` join conditions are those extract filters repeated and
/// the port omits them, keeping the genuine `monthID`, `hourDayID`, `ageID`
/// and `sourceTypeID` joins.
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

/// One `stmyTVVCoeffs` row — the source-type / model-year / fuel-type
/// canister and leak coefficients, aggregated by the `NewTVVYear` setup
/// section from `CumTVVCoeffs`.
///
/// The `NewTVVYear` section sums each `CumTVVCoeffs` coefficient, weighted by
/// the regulatory-class fraction, up to the `(sourceTypeID, modelYearID,
/// fuelTypeID, polProcessID)` grain. TVV-3 reads these onto every
/// tank-vapour-generated row. The `double` columns are schema-nullable but
/// are model data populated for every processed row; modelled as `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StmyTvvCoeffsRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `backPurgeFactor` — the fraction of canister load back-purged each day.
    pub back_purge_factor: f64,
    /// `averageCanisterCapacity` — the canister's vapour-storage capacity.
    pub average_canister_capacity: f64,
    /// `leakFraction` — the leaking-canister fraction (no I/M).
    pub leak_fraction: f64,
    /// `leakFractionIM` — the leaking-canister fraction under I/M.
    pub leak_fraction_im: f64,
    /// `tankSize` — the fuel-tank size.
    pub tank_size: f64,
    /// `tankFillFraction` — the average tank fill fraction.
    pub tank_fill_fraction: f64,
}

/// One `stmyTVVEquations` row — like [`StmyTvvCoeffsRow`] but retaining the
/// `regClassID` dimension and the `tvvEquation` / `leakEquation` expression
/// names; aggregated by the `NewTVVYear` setup section.
///
/// TVV-5 joins these to the `Tvg` soak recurrence to evaluate the
/// data-driven cumulative-TVV equations (see [`VentingEquations`]) and
/// weights the result by `regClassFractionOfSourceTypeModelYearFuel`.
#[derive(Debug, Clone, PartialEq)]
pub struct StmyTvvEquationsRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `regClassID` — regulatory class.
    pub reg_class_id: i32,
    /// `backPurgeFactor` — the fraction of canister load back-purged each day.
    pub back_purge_factor: f64,
    /// `averageCanisterCapacity` — the canister's vapour-storage capacity.
    pub average_canister_capacity: f64,
    /// `regClassFractionOfSourceTypeModelYearFuel` — the regulatory class's
    /// share of the `(sourceType, modelYear, fuelType)` population.
    pub reg_class_fraction_of_source_type_model_year_fuel: f64,
    /// `tvvEquation` — the name of the cumulative-TVV expression to evaluate;
    /// passed to [`VentingEquations::tvv`].
    pub tvv_equation: String,
    /// `leakEquation` — the name of the leaking-canister expression to
    /// evaluate; passed to [`VentingEquations::leak`].
    pub leak_equation: String,
    /// `leakFraction` — the leaking-canister fraction (no I/M).
    pub leak_fraction: f64,
    /// `leakFractionIM` — the leaking-canister fraction under I/M;
    /// schema-nullable, `None` falls back to `leakFraction` in TVV-5.
    pub leak_fraction_im: Option<f64>,
    /// `tankSize` — the fuel-tank size.
    pub tank_size: f64,
    /// `tankFillFraction` — the average tank fill fraction.
    pub tank_fill_fraction: f64,
}

/// One `TankVaporGenCoeffs` row — the tank-vapor-generated coefficients for
/// an ethanol level and altitude bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TankVaporGenCoeffsRow {
    /// `ethanolLevelID` — the ethanol level (0 or 10) the coefficients
    /// apply to; TVV-4 interpolates between the two.
    pub ethanol_level_id: i32,
    /// `altitude` — the `CHAR(1)` altitude bucket (`'L'` low / `'H'` high);
    /// TVV-3 computes a tank-vapour-generated value for each and interpolates
    /// between them by barometric pressure.
    pub altitude: char,
    /// `tvgTermA` — the multiplicative term of the TVG equation.
    pub tvg_term_a: f64,
    /// `tvgTermB` — the RVP exponential coefficient of the TVG equation.
    pub tvg_term_b: f64,
    /// `tvgTermC` — the temperature exponential coefficient of the TVG
    /// equation.
    pub tvg_term_c: f64,
}

/// One `ZoneMonthHour` row — a zone/month/hour ambient temperature.
///
/// Extracted to the run's single zone, so TVV-8 joins it on `hourID` and
/// reads `monthID`; `zoneID` is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `monthID`.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `temperature` — the ambient temperature (°F).
    pub temperature: f64,
}

/// Inputs to [`MultidayTankVaporVentingCalculator::calculate`] — the tables
/// the SQL's "Extract Data", "NewTVVYear" and "FillSampleVehicleSoaking"
/// sections produce, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct MultidayTankVaporVentingInputs {
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `AverageTankGasoline` rows.
    pub average_tank_gasoline: Vec<AverageTankGasolineRow>,
    /// `ColdSoakInitialHourFraction` rows (all `soakDayID = 1`).
    pub cold_soak_initial_hour_fraction: Vec<ColdSoakInitialHourFractionRow>,
    /// `ColdSoakTankTemperature` rows.
    pub cold_soak_tank_temperature: Vec<ColdSoakTankTemperatureRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `evapTemperatureAdjustment` rows (`processID = 12`).
    pub evap_temperature_adjustment: Vec<EvapTemperatureAdjustmentRow>,
    /// `evapRVPTemperatureAdjustment` rows (`processID = 12`).
    pub evap_rvp_temperature_adjustment: Vec<EvapRvpTemperatureAdjustmentRow>,
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
    /// `RunSpecHourDay` — the `hourDayID`s the run processes; TVV-8's
    /// operating-mode insert cross-joins this.
    pub run_spec_hour_day: Vec<i32>,
    /// `RunSpecSourceType` — the `sourceTypeID`s the run processes; TVV-8's
    /// operating-mode insert joins this to keep run-spec source types only.
    pub run_spec_source_type: Vec<i32>,
    /// `sampleVehicleSoaking` rows.
    pub sample_vehicle_soaking: Vec<SampleVehicleSoakingRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceHours` rows.
    pub source_hours: Vec<SourceHoursRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `stmyTVVCoeffs` rows.
    pub stmy_tvv_coeffs: Vec<StmyTvvCoeffsRow>,
    /// `stmyTVVEquations` rows.
    pub stmy_tvv_equations: Vec<StmyTvvEquationsRow>,
    /// `TankVaporGenCoeffs` rows.
    pub tank_vapor_gen_coeffs: Vec<TankVaporGenCoeffsRow>,
    /// `ZoneMonthHour` rows.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
}

/// The per-run scalar context [`MultidayTankVaporVentingCalculator::calculate`]
/// reads — the `##context.*##` substitutions the SQL preprocessor resolves
/// before running the script.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunContext {
    /// `##context.year##` — the run's calendar year. Used to derive vehicle
    /// age (`year − modelYearID` / `year − ageID`) in TVV-1, TVV-5 and
    /// TVV-8, and stamped as `yearID` on the output in TVV-9.
    pub year: i32,
    /// `##context.monthID##` — the run's month. TVV-7 stamps it onto the
    /// `coldSoakInitialHourFraction` rows it folds in from
    /// `sampleVehicleSoaking` for soaking days beyond the first.
    pub month_id: i32,
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
/// carries the I/M-adjusted emission. Unlike the single-day script, the
/// multi-day TVV-9 carries a `regClassID` dimension.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MultidayTankVaporVentingEmissionRow {
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
    /// `regClassID`.
    pub reg_class_id: i32,
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

impl MultidayTankVaporVentingEmissionRow {
    /// The integer dimension tuple — every column except `emissionQuant`.
    /// Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the TVV-9 `INSERT … SELECT`
    /// has no `ORDER BY`), so the port sorts purely to make the result
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

struct RunSpecHourDayIdRow {
    hour_day_id: i32,
}
struct RunSpecSourceTypeIdRow {
    source_type_id: i32,
}

impl TableRow for RunSpecHourDayIdRow {
    fn table_name() -> &'static str {
        "RunSpecHourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "hourDayID".into(),
                rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecHourDay";
        let col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(RunSpecHourDayIdRow {
                    hour_day_id: col
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "hourDayID", "null value".into()))?,
                })
            })
            .collect()
    }
}

impl TableRow for RunSpecSourceTypeIdRow {
    fn table_name() -> &'static str {
        "RunSpecSourceType"
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
        let t = "RunSpecSourceType";
        let col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(RunSpecSourceTypeIdRow {
                    source_type_id: col
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "sourceTypeID", "null value".into()))?,
                })
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// TableRow impls for all public row types.
// ---------------------------------------------------------------------------

impl TableRow for AgeCategoryRow {
    fn table_name() -> &'static str {
        "AgeCategory"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("ageID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AgeCategory";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let age_id = get_i32("ageID")?;
        let age_group_id = get_i32("ageGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AgeCategoryRow {
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AverageTankGasolineRow {
    fn table_name() -> &'static str {
        "AverageTankGasoline"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("ETOHVolume".into(), DataType::Float64),
            ("RVP".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ETOHVolume".into(),
                    rows.iter().map(|r| r.etoh_volume).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "RVP".into(),
                    rows.iter().map(|r| r.rvp).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AverageTankGasoline";
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
        let ft = get_i32("fuelTypeID")?;
        let mg = get_i32("monthGroupID")?;
        let etoh = get_f64("ETOHVolume")?;
        let rvp = get_f64("RVP")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AverageTankGasolineRow {
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                    etoh_volume: etoh.get(i).ok_or_else(|| null("ETOHVolume"))?,
                    rvp: rvp.get(i).ok_or_else(|| null("RVP"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ColdSoakInitialHourFractionRow {
    fn table_name() -> &'static str {
        "ColdSoakInitialHourFraction"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "initialHourDayID".into(),
                    rows.iter()
                        .map(|r| r.initial_hour_day_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "coldSoakInitialHourFraction".into(),
                    rows.iter()
                        .map(|r| r.cold_soak_initial_hour_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ColdSoakInitialHourFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let st = get_i32("sourceTypeID")?;
        let zone = get_i32("zoneID")?;
        let mo = get_i32("monthID")?;
        let hd = get_i32("hourDayID")?;
        let ihd = get_i32("initialHourDayID")?;
        let frac = df
            .column("coldSoakInitialHourFraction")
            .map_err(|e| row_err(t, 0, "coldSoakInitialHourFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "coldSoakInitialHourFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ColdSoakInitialHourFractionRow {
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    initial_hour_day_id: ihd.get(i).ok_or_else(|| null("initialHourDayID"))?,
                    cold_soak_initial_hour_fraction: frac
                        .get(i)
                        .ok_or_else(|| null("coldSoakInitialHourFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ColdSoakTankTemperatureRow {
    fn table_name() -> &'static str {
        "ColdSoakTankTemperature"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("coldSoakTankTemperature".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "coldSoakTankTemperature".into(),
                    rows.iter()
                        .map(|r| r.cold_soak_tank_temperature)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ColdSoakTankTemperature";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mo = get_i32("monthID")?;
        let hr = get_i32("hourID")?;
        let temp = df
            .column("coldSoakTankTemperature")
            .map_err(|e| row_err(t, 0, "coldSoakTankTemperature", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "coldSoakTankTemperature", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ColdSoakTankTemperatureRow {
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    cold_soak_tank_temperature: temp
                        .get(i)
                        .ok_or_else(|| null("coldSoakTankTemperature"))?,
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
            ("barometricPressure".into(), DataType::Float64),
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
                    "barometricPressure".into(),
                    rows.iter()
                        .map(|r| r.barometric_pressure)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let county_id_col = df
            .column("countyID")
            .map_err(|e| row_err(t, 0, "countyID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "countyID", e.to_string()))?;
        let baro_col = df
            .column("barometricPressure")
            .map_err(|e| row_err(t, 0, "barometricPressure", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "barometricPressure", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CountyRow {
                    county_id: county_id_col.get(i).ok_or_else(|| null("countyID"))?,
                    barometric_pressure: baro_col
                        .get(i)
                        .ok_or_else(|| null("barometricPressure"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionRateByAgeRow {
    fn table_name() -> &'static str {
        "EmissionRateByAge"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIM".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionRateByAge";
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
        let sb = df
            .column("sourceBinID")
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?
            .i64()
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let pp = get_i32("polProcessID")?;
        let om = get_i32("opModeID")?;
        let ag = get_i32("ageGroupID")?;
        let mbr = get_f64("meanBaseRate")?;
        let mbrim = get_f64("meanBaseRateIM")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRateByAgeRow {
                    source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: om.get(i).ok_or_else(|| null("opModeID"))?,
                    age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                    mean_base_rate: mbr.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mbrim.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EvapTemperatureAdjustmentRow {
    fn table_name() -> &'static str {
        "evapTemperatureAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("tempAdjustTerm3".into(), DataType::Float64),
            ("tempAdjustTerm2".into(), DataType::Float64),
            ("tempAdjustTerm1".into(), DataType::Float64),
            ("tempAdjustConstant".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "tempAdjustTerm3".into(),
                    rows.iter()
                        .map(|r| r.temp_adjust_term_3)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTerm2".into(),
                    rows.iter()
                        .map(|r| r.temp_adjust_term_2)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTerm1".into(),
                    rows.iter()
                        .map(|r| r.temp_adjust_term_1)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustConstant".into(),
                    rows.iter()
                        .map(|r| r.temp_adjust_constant)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "evapTemperatureAdjustment";
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let t3 = get_f64("tempAdjustTerm3")?;
        let t2 = get_f64("tempAdjustTerm2")?;
        let t1 = get_f64("tempAdjustTerm1")?;
        let tc = get_f64("tempAdjustConstant")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EvapTemperatureAdjustmentRow {
                    temp_adjust_term_3: t3.get(i).ok_or_else(|| null("tempAdjustTerm3"))?,
                    temp_adjust_term_2: t2.get(i).ok_or_else(|| null("tempAdjustTerm2"))?,
                    temp_adjust_term_1: t1.get(i).ok_or_else(|| null("tempAdjustTerm1"))?,
                    temp_adjust_constant: tc.get(i).ok_or_else(|| null("tempAdjustConstant"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EvapRvpTemperatureAdjustmentRow {
    fn table_name() -> &'static str {
        "evapRVPTemperatureAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("RVP".into(), DataType::Float64),
            ("adjustTerm3".into(), DataType::Float64),
            ("adjustTerm2".into(), DataType::Float64),
            ("adjustTerm1".into(), DataType::Float64),
            ("adjustConstant".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "RVP".into(),
                    rows.iter().map(|r| r.rvp).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "adjustTerm3".into(),
                    rows.iter().map(|r| r.adjust_term_3).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "adjustTerm2".into(),
                    rows.iter().map(|r| r.adjust_term_2).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "adjustTerm1".into(),
                    rows.iter().map(|r| r.adjust_term_1).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "adjustConstant".into(),
                    rows.iter().map(|r| r.adjust_constant).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "evapRVPTemperatureAdjustment";
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let ft = df
            .column("fuelTypeID")
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        let rvp = get_f64("RVP")?;
        let a3 = get_f64("adjustTerm3")?;
        let a2 = get_f64("adjustTerm2")?;
        let a1 = get_f64("adjustTerm1")?;
        let ac = get_f64("adjustConstant")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EvapRvpTemperatureAdjustmentRow {
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    rvp: rvp.get(i).ok_or_else(|| null("RVP"))?,
                    adjust_term_3: a3.get(i).ok_or_else(|| null("adjustTerm3"))?,
                    adjust_term_2: a2.get(i).ok_or_else(|| null("adjustTerm2"))?,
                    adjust_term_1: a1.get(i).ok_or_else(|| null("adjustTerm1"))?,
                    adjust_constant: ac.get(i).ok_or_else(|| null("adjustConstant"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelTypeRow {
    fn table_name() -> &'static str {
        "FuelType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("subjectToEvapCalculations".into(), DataType::Boolean),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "subjectToEvapCalculations".into(),
                    rows.iter()
                        .map(|r| r.subject_to_evap_calculations)
                        .collect::<Vec<bool>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelType";
        let ft = df
            .column("fuelTypeID")
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        let evap = df
            .column("subjectToEvapCalculations")
            .map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?
            .bool()
            .map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelTypeRow {
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    subject_to_evap_calculations: evap
                        .get(i)
                        .ok_or_else(|| null("subjectToEvapCalculations"))?,
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
        let hd = get_i32("hourDayID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ImCoverageRow {
    fn table_name() -> &'static str {
        "IMCoverage"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inspectFreq".into(),
                    rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "testStandardsID".into(),
                    rows.iter()
                        .map(|r| r.test_standards_id)
                        .collect::<Vec<i32>>(),
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
                    "begModelYearID".into(),
                    rows.iter()
                        .map(|r| r.beg_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "complianceFactor".into(),
                    rows.iter()
                        .map(|r| r.compliance_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMCoverage";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?;
        let inf = get_i32("inspectFreq")?;
        let ts = get_i32("testStandardsID")?;
        let st = get_i32("sourceTypeID")?;
        let ft = get_i32("fuelTypeID")?;
        let bmy = get_i32("begModelYearID")?;
        let emy = get_i32("endModelYearID")?;
        let cf = df
            .column("complianceFactor")
            .map_err(|e| row_err(t, 0, "complianceFactor", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "complianceFactor", e.to_string()))?;
        (0..df.height())
            .map(|i| {
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
            })
            .collect()
    }
}

impl TableRow for ImFactorRow {
    fn table_name() -> &'static str {
        "IMFactor"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inspectFreq".into(),
                    rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "testStandardsID".into(),
                    rows.iter()
                        .map(|r| r.test_standards_id)
                        .collect::<Vec<i32>>(),
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
                    "IMModelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.im_model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "IMFactor".into(),
                    rows.iter().map(|r| r.im_factor).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMFactor";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?;
        let inf = get_i32("inspectFreq")?;
        let ts = get_i32("testStandardsID")?;
        let st = get_i32("sourceTypeID")?;
        let ft = get_i32("fuelTypeID")?;
        let imyg = get_i32("IMModelYearGroupID")?;
        let ag = get_i32("ageGroupID")?;
        let imf = df
            .column("IMFactor")
            .map_err(|e| row_err(t, 0, "IMFactor", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "IMFactor", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ImFactorRow {
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    inspect_freq: inf.get(i).ok_or_else(|| null("inspectFreq"))?,
                    test_standards_id: ts.get(i).ok_or_else(|| null("testStandardsID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    im_model_year_group_id: imyg
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
                    age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                    im_factor: imf.get(i).ok_or_else(|| null("IMFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for MonthOfAnyYearRow {
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
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mo = get_i32("monthID")?;
        let mg = get_i32("monthGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MonthOfAnyYearRow {
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OpModeDistributionRow {
    fn table_name() -> &'static str {
        "OpModeDistribution"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let st = get_i32("sourceTypeID")?;
        let hd = get_i32("hourDayID")?;
        let lk = get_i32("linkID")?;
        let pp = get_i32("polProcessID")?;
        let om = get_i32("opModeID")?;
        let omf = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModeDistributionRow {
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    link_id: lk.get(i).ok_or_else(|| null("linkID"))?,
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: om.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: omf.get(i).ok_or_else(|| null("opModeFraction"))?,
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
        let pp = get_i32("polProcessID")?;
        let proc = get_i32("processID")?;
        let poll = get_i32("pollutantID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    pollutant_id: poll.get(i).ok_or_else(|| null("pollutantID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PollutantProcessModelYearRow {
    fn table_name() -> &'static str {
        "PollutantProcessModelYear"
    }
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
                Series::new(
                    "IMModelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.im_model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?;
        let my = get_i32("modelYearID")?;
        let myg = get_i32("modelYearGroupID")?;
        let imyg = get_i32("IMModelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessModelYearRow {
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                    im_model_year_group_id: imyg
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SampleVehicleSoakingRow {
    fn table_name() -> &'static str {
        "sampleVehicleSoaking"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("soakDayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("soakFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "soakDayID".into(),
                    rows.iter().map(|r| r.soak_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
                    "soakFraction".into(),
                    rows.iter().map(|r| r.soak_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sampleVehicleSoaking";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let sd = get_i32("soakDayID")?;
        let st = get_i32("sourceTypeID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let sf = df
            .column("soakFraction")
            .map_err(|e| row_err(t, 0, "soakFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "soakFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SampleVehicleSoakingRow {
                    soak_day_id: sd.get(i).ok_or_else(|| null("soakDayID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    soak_fraction: sf.get(i).ok_or_else(|| null("soakFraction"))?,
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
            ("regClassID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
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
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
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
        let t = "SourceBin";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let sb = df
            .column("sourceBinID")
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?
            .i64()
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let ft = get_i32("fuelTypeID")?;
        let rc = get_i32("regClassID")?;
        let myg = get_i32("modelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinRow {
                    source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
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
        let pp = get_i32("polProcessID")?;
        let sb = df
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
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: sb.get(i).ok_or_else(|| null("sourceBinID"))?,
                    source_bin_activity_fraction: sbaf
                        .get(i)
                        .ok_or_else(|| null("sourceBinActivityFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str {
        "SourceHours"
    }
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
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceHours".into(),
                    rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceHours";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hd = get_i32("hourDayID")?;
        let mo = get_i32("monthID")?;
        let age = get_i32("ageID")?;
        let st = get_i32("sourceTypeID")?;
        let sh = df
            .column("sourceHours")
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceHoursRow {
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_hours: sh.get(i).ok_or_else(|| null("sourceHours"))?,
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
            ("modelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
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
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
        let my = get_i32("modelYearID")?;
        let st = get_i32("sourceTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeModelYearRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StmyTvvCoeffsRow {
    fn table_name() -> &'static str {
        "stmyTVVCoeffs"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("backPurgeFactor".into(), DataType::Float64),
            ("averageCanisterCapacity".into(), DataType::Float64),
            ("leakFraction".into(), DataType::Float64),
            ("leakFractionIM".into(), DataType::Float64),
            ("tankSize".into(), DataType::Float64),
            ("tankFillFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "backPurgeFactor".into(),
                    rows.iter()
                        .map(|r| r.back_purge_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "averageCanisterCapacity".into(),
                    rows.iter()
                        .map(|r| r.average_canister_capacity)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "leakFraction".into(),
                    rows.iter().map(|r| r.leak_fraction).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "leakFractionIM".into(),
                    rows.iter()
                        .map(|r| r.leak_fraction_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankSize".into(),
                    rows.iter().map(|r| r.tank_size).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankFillFraction".into(),
                    rows.iter()
                        .map(|r| r.tank_fill_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "stmyTVVCoeffs";
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
        let st = get_i32("sourceTypeID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        let pp = get_i32("polProcessID")?;
        let bp = get_f64("backPurgeFactor")?;
        let acc = get_f64("averageCanisterCapacity")?;
        let lf = get_f64("leakFraction")?;
        let lfim = get_f64("leakFractionIM")?;
        let ts = get_f64("tankSize")?;
        let tff = get_f64("tankFillFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StmyTvvCoeffsRow {
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    back_purge_factor: bp.get(i).ok_or_else(|| null("backPurgeFactor"))?,
                    average_canister_capacity: acc
                        .get(i)
                        .ok_or_else(|| null("averageCanisterCapacity"))?,
                    leak_fraction: lf.get(i).ok_or_else(|| null("leakFraction"))?,
                    leak_fraction_im: lfim.get(i).ok_or_else(|| null("leakFractionIM"))?,
                    tank_size: ts.get(i).ok_or_else(|| null("tankSize"))?,
                    tank_fill_fraction: tff.get(i).ok_or_else(|| null("tankFillFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StmyTvvEquationsRow {
    fn table_name() -> &'static str {
        "stmyTVVEquations"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("backPurgeFactor".into(), DataType::Float64),
            ("averageCanisterCapacity".into(), DataType::Float64),
            (
                "regClassFractionOfSourceTypeModelYearFuel".into(),
                DataType::Float64,
            ),
            ("tvvEquation".into(), DataType::String),
            ("leakEquation".into(), DataType::String),
            ("leakFraction".into(), DataType::Float64),
            ("leakFractionIM".into(), DataType::Float64),
            ("tankSize".into(), DataType::Float64),
            ("tankFillFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "backPurgeFactor".into(),
                    rows.iter()
                        .map(|r| r.back_purge_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "averageCanisterCapacity".into(),
                    rows.iter()
                        .map(|r| r.average_canister_capacity)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "regClassFractionOfSourceTypeModelYearFuel".into(),
                    rows.iter()
                        .map(|r| r.reg_class_fraction_of_source_type_model_year_fuel)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tvvEquation".into(),
                    rows.iter()
                        .map(|r| r.tvv_equation.clone())
                        .collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "leakEquation".into(),
                    rows.iter()
                        .map(|r| r.leak_equation.clone())
                        .collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "leakFraction".into(),
                    rows.iter().map(|r| r.leak_fraction).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "leakFractionIM".into(),
                    rows.iter()
                        .map(|r| r.leak_fraction_im.unwrap_or(f64::NAN))
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankSize".into(),
                    rows.iter().map(|r| r.tank_size).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankFillFraction".into(),
                    rows.iter()
                        .map(|r| r.tank_fill_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "stmyTVVEquations";
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
        let st = get_i32("sourceTypeID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        let pp = get_i32("polProcessID")?;
        let rc = get_i32("regClassID")?;
        let bp = get_f64("backPurgeFactor")?;
        let acc = get_f64("averageCanisterCapacity")?;
        let rcf = get_f64("regClassFractionOfSourceTypeModelYearFuel")?;
        let tvveq_col = df
            .column("tvvEquation")
            .map_err(|e| row_err(t, 0, "tvvEquation", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "tvvEquation", e.to_string()))?;
        let leakeq_col = df
            .column("leakEquation")
            .map_err(|e| row_err(t, 0, "leakEquation", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "leakEquation", e.to_string()))?;
        let lf = get_f64("leakFraction")?;
        let lfim = get_f64("leakFractionIM")?;
        let ts = get_f64("tankSize")?;
        let tff = get_f64("tankFillFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StmyTvvEquationsRow {
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    back_purge_factor: bp.get(i).ok_or_else(|| null("backPurgeFactor"))?,
                    average_canister_capacity: acc
                        .get(i)
                        .ok_or_else(|| null("averageCanisterCapacity"))?,
                    reg_class_fraction_of_source_type_model_year_fuel: rcf
                        .get(i)
                        .ok_or_else(|| null("regClassFractionOfSourceTypeModelYearFuel"))?,
                    tvv_equation: tvveq_col
                        .get(i)
                        .ok_or_else(|| null("tvvEquation"))?
                        .to_string(),
                    leak_equation: leakeq_col
                        .get(i)
                        .ok_or_else(|| null("leakEquation"))?
                        .to_string(),
                    leak_fraction: lf.get(i).ok_or_else(|| null("leakFraction"))?,
                    leak_fraction_im: lfim.get(i).filter(|v| v.is_finite()),
                    tank_size: ts.get(i).ok_or_else(|| null("tankSize"))?,
                    tank_fill_fraction: tff.get(i).ok_or_else(|| null("tankFillFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for TankVaporGenCoeffsRow {
    fn table_name() -> &'static str {
        "TankVaporGenCoeffs"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "ethanolLevelID".into(),
                    rows.iter()
                        .map(|r| r.ethanol_level_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "altitude".into(),
                    rows.iter()
                        .map(|r| r.altitude.to_string())
                        .collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "tvgTermA".into(),
                    rows.iter().map(|r| r.tvg_term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tvgTermB".into(),
                    rows.iter().map(|r| r.tvg_term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tvgTermC".into(),
                    rows.iter().map(|r| r.tvg_term_c).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "TankVaporGenCoeffs";
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
        let el = get_i32("ethanolLevelID")?;
        let alt_col = df
            .column("altitude")
            .map_err(|e| row_err(t, 0, "altitude", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "altitude", e.to_string()))?;
        let ta = get_f64("tvgTermA")?;
        let tb = get_f64("tvgTermB")?;
        let tc = get_f64("tvgTermC")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                let altitude = alt_col
                    .get(i)
                    .ok_or_else(|| null("altitude"))?
                    .chars()
                    .next()
                    .ok_or_else(|| row_err(t, i, "altitude", "empty string".into()))?;
                Ok(TankVaporGenCoeffsRow {
                    ethanol_level_id: el.get(i).ok_or_else(|| null("ethanolLevelID"))?,
                    altitude,
                    tvg_term_a: ta.get(i).ok_or_else(|| null("tvgTermA"))?,
                    tvg_term_b: tb.get(i).ok_or_else(|| null("tvgTermB"))?,
                    tvg_term_c: tc.get(i).ok_or_else(|| null("tvgTermC"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ZoneMonthHourRow {
    fn table_name() -> &'static str {
        "ZoneMonthHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("temperature".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "temperature".into(),
                    rows.iter().map(|r| r.temperature).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ZoneMonthHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mo = get_i32("monthID")?;
        let hr = get_i32("hourID")?;
        let temp = df
            .column("temperature")
            .map_err(|e| row_err(t, 0, "temperature", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "temperature", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneMonthHourRow {
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    temperature: temp.get(i).ok_or_else(|| null("temperature"))?,
                })
            })
            .collect()
    }
}

impl TableRow for MultidayTankVaporVentingEmissionRow {
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
        let yr = get_i32("yearID")?;
        let mo = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let poll = get_i32("pollutantID")?;
        let proc = get_i32("processID")?;
        let st = get_i32("sourceTypeID")?;
        let rc = get_i32("regClassID")?;
        let ft = get_i32("fuelTypeID")?;
        let my = get_i32("modelYearID")?;
        let rt = get_i32("roadTypeID")?;
        let eq = df
            .column("emissionQuant")
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MultidayTankVaporVentingEmissionRow {
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
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: rt.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
                })
            })
            .collect()
    }
}

/// The measures of one joined `stmyTVVEquations ⋈ TVG` row, passed to a
/// [`VentingEquations`] evaluator.
///
/// TVV-5's `##tvvEquations##` / `##leakEquations##` `CASE` expressions are
/// arbitrary SQL the MOVES default database supplies; they reference the
/// columns of the joined coefficient and soak-recurrence rows. This struct
/// carries every measure such an expression can read, so an evaluator can
/// reproduce whatever formula the default database holds. See the module
/// "Data-driven equations" section.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EquationVars {
    /// `TVG.soakDayID` — the soaking-day index.
    pub soak_day_id: i32,
    /// `TVG.TVGdaily` — the first-day tank vapour generated.
    pub tvg_daily: f64,
    /// `TVG.Xn` — the running canister load for this soaking day.
    pub xn: f64,
    /// `TVG.tvgSum1H` — the TVG summed from the day's first hour to hour H.
    pub tvg_sum_1h: f64,
    /// `TVG.tvgSumH24` — the TVG summed from after hour H to the day's end.
    pub tvg_sum_h24: f64,
    /// `stmyTVVEquations.backPurgeFactor` (equals `TVG.backPurgeFactor`).
    pub back_purge_factor: f64,
    /// `stmyTVVEquations.averageCanisterCapacity`.
    pub average_canister_capacity: f64,
    /// `stmyTVVEquations.tankSize`.
    pub tank_size: f64,
    /// `stmyTVVEquations.tankFillFraction`.
    pub tank_fill_fraction: f64,
}

/// Evaluator for TVV-5's data-driven `tvvEquation` / `leakEquation`
/// expressions.
///
/// The MOVES default database stores the cumulative-TVV and leaking-canister
/// formulae as SQL expression strings in `cumTVVCoeffs`; the Java substitutes
/// them verbatim into a `CASE` and never interprets them.
/// [`MultidayTankVaporVentingCalculator::calculate`] is given an
/// implementation of this trait and calls it once per joined coefficient /
/// soak-recurrence row, passing the equation name (`tvvEquation` /
/// `leakEquation`) and the row's [`EquationVars`]. See the module
/// "Data-driven equations" section.
pub trait VentingEquations {
    /// Evaluate the `tvvEquation` named `equation` against `vars` — the
    /// `##tvvEquations##` `CASE`. An unknown name resolves to `0.0` (the
    /// Java's `case … else 0 end`).
    fn tvv(&self, equation: &str, vars: &EquationVars) -> f64;

    /// Evaluate the `leakEquation` named `equation` against `vars` — the
    /// `##leakEquations##` `CASE`. An unknown name resolves to `0.0`.
    fn leak(&self, equation: &str, vars: &EquationVars) -> f64;
}

// ===========================================================================
// Working tables — private mirrors of the MyISAM tables the SQL's
// "Processing" section builds and drops. Each numbered TVV step (and the
// intervening TVG soak recurrence) produces one; later steps consume it.
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

/// TVV-3 working table — `TankVaporGenerated`, after the barometric-pressure
/// interpolation of the high- and low-altitude values. Tank vapour generated
/// for one `(currentHour, initialHour)` cold-soak pair at one ethanol level,
/// per `(modelYear, polProcess)`.
///
/// The SQL's `TankVaporGenerated` / `EthanolWeightedTVG` / `TVG` tables also
/// carry `leakFraction` / `leakFractionIM` columns, but no step reads them —
/// TVV-5 takes the leak fractions from `stmyTVVEquations` — so the port omits
/// those dead columns from this chain of working tables.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TankVaporGenerated {
    hour_day_id: i32,
    initial_hour_day_id: i32,
    ethanol_level_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    tank_vapor_generated: f64,
    back_purge_factor: f64,
    average_canister_capacity: f64,
}

/// TVV-4 working table — `EthanolWeightedTVG`. Tank vapour generated, linearly
/// interpolated between the 0- and 10-percent ethanol levels and differenced
/// from the previous hour into an hourly increment.
#[derive(Debug, Clone, Copy, PartialEq)]
struct EthanolWeightedTvg {
    hour_day_id: i32,
    initial_hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    ethanol_weighted_tvg: f64,
    back_purge_factor: f64,
    average_canister_capacity: f64,
}

/// The TVG soak recurrence working table — `TVG`. The running canister load
/// `Xn` for one `(soakDay, currentHour, initialHour)` triple, with the
/// first-day total `TVGdaily` and the two day-window partial sums the
/// recurrence carries.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Tvg {
    soak_day_id: i32,
    hour_day_id: i32,
    initial_hour_day_id: i32,
    month_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    tvg_daily: f64,
    xn: f64,
    back_purge_factor: f64,
    average_canister_capacity: f64,
    tvg_sum_1h: f64,
    tvg_sum_h24: f64,
}

/// TVV-5 working table — `CummulativeTankVaporVented`. Cumulative tank vapour
/// vented (base and I/M) by regulatory class and vehicle age, with the prior
/// hour pre-computed for TVV-6's self-join.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CumulativeTankVaporVented {
    soak_day_id: i32,
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
/// of cumulative TVV, floored at zero and zeroed when the tank is not warming.
#[derive(Debug, Clone, Copy, PartialEq)]
struct UnweightedHourlyTvv {
    zone_id: i32,
    soak_day_id: i32,
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
/// TVV, summed across the `soakDayID` and `initialHourDayID` dimensions, plus
/// the four post-peak decay hours.
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
/// weighted mean base rate (base and I/M) per operating mode, with the
/// temperature / RVP adjustment applied to the operating mode.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WeightedMeanBaseRate {
    pol_process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
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
/// For each `(process, pollutant, modelYear, fuelType, sourceType)` the step
/// sums `IMFactor × complianceFactor × 0.01` over every I/M coverage row that
/// applies. `PollutantProcessModelYear` is joined to `PollutantProcessAssoc`
/// (`polProcessID`), to `IMFactor` (`polProcessID, IMModelYearGroupID`), to
/// `AgeCategory` (`ageGroupID`, with the model year pinned to `year − ageID`),
/// and to `IMCoverage` (the five-column [`ImJoinKey`] plus the coverage's
/// `[begModelYearID, endModelYearID]` model-year range). Identical to the
/// single-day script's TVV-1. The `IMCoverage.countyID` / `yearID` and
/// `polProcessID IN (…)` clauses repeat "Extract Data" filters and are
/// omitted.
fn im_coverage_merged_ungrouped(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
) -> Vec<ImCoverageMerged> {
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
/// `round(T,2)*100000 + (999 − hourID)`, unpacked with `mod(…,1000)`. The port
/// ranks each row by the exact-integer tuple `(round(T × 100), −hourID)`,
/// the same ordering without the float noise the SQL's `mod` is exposed to.
/// Identical to the single-day script's TVV-2.
fn peak_hour_of_cold_soak(inputs: &MultidayTankVaporVentingInputs) -> Vec<PeakHourOfColdSoak> {
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

/// The barometric-pressure interpolation endpoints, verbatim from the SQL:
/// low altitude is Wayne County, MI (`29.069` inHg), high altitude is Denver
/// County, CO (`24.087` inHg).
const LOW_ALTITUDE_PRESSURE: f64 = 29.069;
/// See [`LOW_ALTITUDE_PRESSURE`].
const HIGH_ALTITUDE_PRESSURE: f64 = 24.087;

/// The eight-column key identifying a `TankVaporGeneratedHighAndLow` row
/// within one altitude: `(hourDayID, initialHourDayID, ethanolLevelID,
/// monthID, sourceTypeID, fuelTypeID, modelYearID, polProcessID)`.
type TvgHighLowKey = (i32, i32, i32, i32, i32, i32, i32, i32);

/// TVV-3 — calculate `TankVaporGenerated` (TVG) by ethanol level.
///
/// For each `ColdSoakInitialHourFraction` row with a positive fraction, the
/// step pairs the current-hour temperature `t2` with the cold-soak initial
/// hour's temperature `t1`, keeps hours up to the month's peak cold-soak hour,
/// and — cross-joining `TankVaporGenCoeffs` — computes, for **each altitude**,
///
/// ```text
/// TVG = tankSize · (1 − tankFillFraction)
///       · tvgTermA · exp(tvgTermB · RVP)
///       · (exp(tvgTermC · t2) − exp(tvgTermC · t1))
/// ```
///
/// clamped to `0` when the tank is warming (`t1 ≥ t2`). The
/// `tankSize · (1 − tankFillFraction)` prefactor and the canister
/// coefficients come from `stmyTVVCoeffs`, joined on `(sourceType,
/// fuelType)` — so a row exists per `(modelYear, polProcess)`.
///
/// The high- and low-altitude values are then interpolated by the county's
/// barometric pressure (the `TankVaporGenerated` insert):
///
/// ```text
/// TVG = greatest(((baroP − 29.069) / (24.087 − 29.069))
///                · (TVG_high − TVG_low) + TVG_low, 0)
/// ```
///
/// The single-day script picked one altitude's coefficients by the county's
/// altitude bucket; the multi-day script interpolates. The
/// `(hourID = 1, initialHour = 1, same day)` self-pair is admitted (it yields
/// `TVG = 0` since `t1 = t2`) so the soak recurrence has an hour-1 row.
fn tank_vapor_generated(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
    peak_hours: &[PeakHourOfColdSoak],
) -> Vec<TankVaporGenerated> {
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
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
    let mut coeffs_by_st_ft: HashMap<(i32, i32), Vec<&StmyTvvCoeffsRow>> = HashMap::new();
    for sc in &inputs.stmy_tvv_coeffs {
        coeffs_by_st_ft
            .entry((sc.source_type_id, sc.fuel_type_id))
            .or_default()
            .push(sc);
    }

    // TankVaporGeneratedHighAndLow, keyed by altitude flag then the 8-column
    // identity. `INSERT` collisions cannot occur — each combination is
    // produced once — so a plain map suffices.
    let mut low: HashMap<TvgHighLowKey, (f64, &StmyTvvCoeffsRow)> = HashMap::new();
    let mut high: HashMap<TvgHighLowKey, (f64, &StmyTvvCoeffsRow)> = HashMap::new();

    for ihf in &inputs.cold_soak_initial_hour_fraction {
        // WHERE coldSoakInitialHourFraction > 0.
        if ihf.cold_soak_initial_hour_fraction <= 0.0 {
            continue;
        }
        let (Some(hd), Some(ihd)) = (
            hour_day_of.get(&ihf.hour_day_id),
            hour_day_of.get(&ihf.initial_hour_day_id),
        ) else {
            continue;
        };
        // WHERE hourDayID <> initialHourDayID OR (both are hour 1, same day).
        let distinct = ihf.hour_day_id != ihf.initial_hour_day_id;
        let hour1_self = hd.hour_id == 1 && ihd.hour_id == 1 && hd.day_id == ihd.day_id;
        if !(distinct || hour1_self) {
            continue;
        }
        let Some(&peak) = peak_of.get(&ihf.month_id) else {
            continue;
        };
        // WHERE hd.hourID <= peakHourID.
        if hd.hour_id > peak {
            continue;
        }
        let (Some(&t2), Some(&t1)) = (
            temp_of.get(&(ihf.month_id, hd.hour_id)),
            temp_of.get(&(ihf.month_id, ihd.hour_id)),
        ) else {
            continue;
        };
        let Some(&month_group_id) = month_group_of.get(&ihf.month_id) else {
            continue;
        };
        let Some(gasoline_list) = gasoline_by_group.get(&month_group_id) else {
            continue;
        };
        for gas in gasoline_list {
            let Some(coeffs_list) = coeffs_by_st_ft.get(&(ihf.source_type_id, gas.fuel_type_id))
            else {
                continue;
            };
            for sc in coeffs_list {
                let prefactor = sc.tank_size * (1.0 - sc.tank_fill_fraction);
                for tvg in &inputs.tank_vapor_gen_coeffs {
                    let value = if t1 >= t2 {
                        0.0
                    } else {
                        prefactor
                            * tvg.tvg_term_a
                            * (tvg.tvg_term_b * gas.rvp).exp()
                            * ((tvg.tvg_term_c * t2).exp() - (tvg.tvg_term_c * t1).exp())
                    };
                    let key: TvgHighLowKey = (
                        ihf.hour_day_id,
                        ihf.initial_hour_day_id,
                        tvg.ethanol_level_id,
                        ihf.month_id,
                        ihf.source_type_id,
                        gas.fuel_type_id,
                        sc.model_year_id,
                        sc.pol_process_id,
                    );
                    match tvg.altitude {
                        'L' => {
                            low.insert(key, (value, sc));
                        }
                        'H' => {
                            high.insert(key, (value, sc));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // TankVaporGenerated — interpolate the two altitudes by barometric
    // pressure. `County` is cross-joined; the run has one county.
    let baro = inputs
        .county
        .iter()
        .find(|c| c.county_id == ctx.county_id)
        .map(|c| c.barometric_pressure);
    let Some(baro) = baro else {
        return Vec::new();
    };
    let factor = (baro - LOW_ALTITUDE_PRESSURE) / (HIGH_ALTITUDE_PRESSURE - LOW_ALTITUDE_PRESSURE);

    let mut out = Vec::new();
    for (key, (low_value, sc)) in &low {
        let Some(&(high_value, _)) = high.get(key) else {
            continue;
        };
        let (
            hour_day_id,
            initial_hour_day_id,
            ethanol_level_id,
            month_id,
            source_type_id,
            fuel_type_id,
            model_year_id,
            pol_process_id,
        ) = *key;
        out.push(TankVaporGenerated {
            hour_day_id,
            initial_hour_day_id,
            ethanol_level_id,
            month_id,
            source_type_id,
            fuel_type_id,
            model_year_id,
            pol_process_id,
            tank_vapor_generated: (factor * (high_value - low_value) + low_value).max(0.0),
            back_purge_factor: sc.back_purge_factor,
            average_canister_capacity: sc.average_canister_capacity,
        });
    }
    out
}

/// The seven-column key pairing the ethanol-level-0 and -10 rows and keying
/// the hourly-increment difference: `(initialHourDayID, monthID, sourceTypeID,
/// fuelTypeID, modelYearID, polProcessID)` plus a per-call hour-day component.
type TvgPairKey = (i32, i32, i32, i32, i32, i32);

/// TVV-4 — calculate ethanol-weighted, hourly-incremental TVG.
///
/// Each ethanol-level-0 [`TankVaporGenerated`] row is paired with its
/// ethanol-level-10 sibling and linearly interpolated by the fuel's ethanol
/// volume:
///
/// ```text
/// cumulativeTVG = TVG₁₀ · f + TVG₀ · (1 − f),  f = least(10, ETOHVolume) / 10
/// ```
///
/// `ETOHVolume` is read from `AverageTankGasoline` through `MonthOfAnyYear`.
/// The multi-day script then differences that (cumulative) quantity into an
/// **hourly increment** — `greatest(0, cumulativeTVG[h] − cumulativeTVG[h−1])`
/// — pairing each row with the same dimensions one hour earlier on the same
/// day (a missing prior hour contributes `0`).
fn ethanol_weighted_tvg(
    inputs: &MultidayTankVaporVentingInputs,
    generated: &[TankVaporGenerated],
) -> Vec<EthanolWeightedTvg> {
    let mut level_ten: HashMap<(i32, TvgPairKey), &TankVaporGenerated> = HashMap::new();
    for g in generated {
        if g.ethanol_level_id == 10 {
            level_ten.insert(
                (
                    g.hour_day_id,
                    (
                        g.initial_hour_day_id,
                        g.month_id,
                        g.source_type_id,
                        g.fuel_type_id,
                        g.model_year_id,
                        g.pol_process_id,
                    ),
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
    let etoh_of: HashMap<(i32, i32), f64> = inputs
        .average_tank_gasoline
        .iter()
        .map(|g| ((g.month_group_id, g.fuel_type_id), g.etoh_volume))
        .collect();

    // EthanolWeightedTVGTemp — the cumulative ethanol-weighted TVG, carrying
    // the decoded (hourID, dayID) the hourly difference keys on.
    struct Temp {
        hour_id: i32,
        day_id: i32,
        tvg: f64,
        carry: TankVaporGenerated,
    }
    let mut temp: Vec<Temp> = Vec::new();
    for t0 in generated {
        if t0.ethanol_level_id != 0 {
            continue;
        }
        let pair = (
            t0.initial_hour_day_id,
            t0.month_id,
            t0.source_type_id,
            t0.fuel_type_id,
            t0.model_year_id,
            t0.pol_process_id,
        );
        let Some(t10) = level_ten.get(&(t0.hour_day_id, pair)) else {
            continue;
        };
        let Some(&month_group_id) = month_group_of.get(&t10.month_id) else {
            continue;
        };
        let Some(&etoh_volume) = etoh_of.get(&(month_group_id, t10.fuel_type_id)) else {
            continue;
        };
        let fraction = etoh_volume.min(10.0) / 10.0;
        let cumulative =
            t10.tank_vapor_generated * fraction + t0.tank_vapor_generated * (1.0 - fraction);
        temp.push(Temp {
            // floor(hourDayID/10), mod(hourDayID,10) — the SQL's decode.
            hour_id: t0.hour_day_id / 10,
            day_id: t0.hour_day_id % 10,
            tvg: cumulative,
            carry: *t0,
        });
    }

    // Difference into hourly increments: prior hour = same day, hourID − 1,
    // same (initialHourDayID, monthID, sourceTypeID, fuelTypeID, modelYearID,
    // polProcessID).
    let prior_of: HashMap<(i32, i32, TvgPairKey), f64> = temp
        .iter()
        .map(|t| {
            (
                (
                    t.hour_id,
                    t.day_id,
                    (
                        t.carry.initial_hour_day_id,
                        t.carry.month_id,
                        t.carry.source_type_id,
                        t.carry.fuel_type_id,
                        t.carry.model_year_id,
                        t.carry.pol_process_id,
                    ),
                ),
                t.tvg,
            )
        })
        .collect();

    temp.iter()
        .map(|t| {
            let pair = (
                t.carry.initial_hour_day_id,
                t.carry.month_id,
                t.carry.source_type_id,
                t.carry.fuel_type_id,
                t.carry.model_year_id,
                t.carry.pol_process_id,
            );
            let prior = prior_of
                .get(&(t.hour_id - 1, t.day_id, pair))
                .copied()
                .unwrap_or(0.0);
            EthanolWeightedTvg {
                hour_day_id: t.carry.hour_day_id,
                initial_hour_day_id: t.carry.initial_hour_day_id,
                month_id: t.carry.month_id,
                source_type_id: t.carry.source_type_id,
                fuel_type_id: t.carry.fuel_type_id,
                model_year_id: t.carry.model_year_id,
                pol_process_id: t.carry.pol_process_id,
                ethanol_weighted_tvg: (t.tvg - prior).max(0.0),
                back_purge_factor: t.carry.back_purge_factor,
                average_canister_capacity: t.carry.average_canister_capacity,
            }
        })
        .collect()
}

/// The seven-column identity of an [`EthanolWeightedTvg`] row and of the
/// `tvgSum*` partial-sum tables: `(hourDayID, initialHourDayID, monthID,
/// sourceTypeID, fuelTypeID, modelYearID, polProcessID)`.
type Key7 = (i32, i32, i32, i32, i32, i32, i32);

/// The TVG soak recurrence — port of the SQL's `tvgSum*` / `TVG` block.
///
/// The multi-day script tracks a vehicle's canister load across successive
/// soaking days. It first builds four day-window partial sums of the hourly
/// [`EthanolWeightedTvg`] increment, each keyed by the [`Key7`] identity:
///
/// * `tvgSumIH` — from the cold-soak initial hour `I` to the current hour `H`
///   (`0` when `H` precedes `I`);
/// * `tvgSumI24` — from `I` to the day's last hour;
/// * `tvgSum1H` — from the day's first hour to `H`, over the rows whose
///   initial hour is hour 1;
/// * `tvgSumH24` — from after `H` to the day's last hour.
///
/// It then fills the `TVG` table soaking day by soaking day:
///
/// ```text
/// day 1:  TVGdaily = Xn = tvgSumIH
/// day 2:  Xn = (1 − backPurgeFactor)·least(tvgSumI24, canisterCap) + tvgSum1H
/// day n:  Xn = (1 − backPurgeFactor)·least(Xnₙ₋₁ + tvgSumH24, canisterCap)
///              + tvgSum1H
/// ```
///
/// with `TVGdaily` carried forward unchanged from day 1. Each day's carry-over
/// canister load is back-purged and capped at the canister capacity. The
/// soaking days iterated beyond day 2 are the distinct `soakDayID > 2` values
/// in `sampleVehicleSoaking`; day `n`'s recurrence reads day `n − 1`, so a gap
/// in those values truncates the recurrence exactly as the SQL `loop` would.
fn tvg_soak_recurrence(
    inputs: &MultidayTankVaporVentingInputs,
    weighted: &[EthanolWeightedTvg],
) -> Vec<Tvg> {
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let hour_day_id_of: HashMap<(i32, i32), i32> = inputs
        .hour_day
        .iter()
        .map(|hd| ((hd.day_id, hd.hour_id), hd.hour_day_id))
        .collect();
    let ewtvg_of: HashMap<Key7, f64> = weighted
        .iter()
        .map(|e| {
            (
                (
                    e.hour_day_id,
                    e.initial_hour_day_id,
                    e.month_id,
                    e.source_type_id,
                    e.fuel_type_id,
                    e.model_year_id,
                    e.pol_process_id,
                ),
                e.ethanol_weighted_tvg,
            )
        })
        .collect();

    // Sum the hourly EthanolWeightedTVG over the hours `lo..=hi` of `day`,
    // for the rows carrying `init_hour_day_id`. `None` when the `inner join`
    // would find no row (empty window or no matching row) — mirrored so the
    // recurrence's `inner join`s drop the same rows the SQL drops.
    let window_sum = |day: i32, lo: i32, hi: i32, init_hour_day_id: i32, e: &EthanolWeightedTvg| {
        let mut sum = 0.0;
        let mut found = false;
        for hour in lo..=hi {
            let Some(&hour_day_id) = hour_day_id_of.get(&(day, hour)) else {
                continue;
            };
            if let Some(&value) = ewtvg_of.get(&(
                hour_day_id,
                init_hour_day_id,
                e.month_id,
                e.source_type_id,
                e.fuel_type_id,
                e.model_year_id,
                e.pol_process_id,
            )) {
                sum += value;
                found = true;
            }
        }
        found.then_some(sum)
    };

    // Soak day 1 — TVGdaily = Xn = tvgSumIH; carry tvgSum1H, tvgSumH24.
    let mut day: Vec<Tvg> = Vec::new();
    for e in weighted {
        let (Some(hd), Some(ihd)) = (
            hour_day_of.get(&e.hour_day_id),
            hour_day_of.get(&e.initial_hour_day_id),
        ) else {
            continue;
        };
        // tvgSumIH: 0 when H precedes I, else the [I, H] window sum (which
        // always contains `e` itself, so it is never absent).
        let sum_ih = if hd.hour_id >= ihd.hour_id {
            window_sum(hd.day_id, ihd.hour_id, hd.hour_id, e.initial_hour_day_id, e).unwrap_or(0.0)
        } else {
            0.0
        };
        // tvgSum1H: [1, H] over the rows whose initial hour is hour 1.
        let Some(&hour1_hour_day_id) = hour_day_id_of.get(&(hd.day_id, 1)) else {
            continue;
        };
        let Some(sum_1h) = window_sum(hd.day_id, 1, hd.hour_id, hour1_hour_day_id, e) else {
            continue; // inner join tvgSum1H
        };
        // tvgSumH24: (H, 24] — a LEFT JOIN, coalesced to 0.
        let sum_h24 =
            window_sum(hd.day_id, hd.hour_id + 1, 24, e.initial_hour_day_id, e).unwrap_or(0.0);
        day.push(Tvg {
            soak_day_id: 1,
            hour_day_id: e.hour_day_id,
            initial_hour_day_id: e.initial_hour_day_id,
            month_id: e.month_id,
            source_type_id: e.source_type_id,
            fuel_type_id: e.fuel_type_id,
            model_year_id: e.model_year_id,
            pol_process_id: e.pol_process_id,
            tvg_daily: sum_ih,
            xn: sum_ih,
            back_purge_factor: e.back_purge_factor,
            average_canister_capacity: e.average_canister_capacity,
            tvg_sum_1h: sum_1h,
            tvg_sum_h24: sum_h24,
        });
    }
    let mut out: Vec<Tvg> = day.clone();

    // Soak day 2 — Xn = (1 − bp)·least(tvgSumI24, cap) + tvgSum1H.
    let weighted_by_key: HashMap<Key7, &EthanolWeightedTvg> = weighted
        .iter()
        .map(|e| {
            (
                (
                    e.hour_day_id,
                    e.initial_hour_day_id,
                    e.month_id,
                    e.source_type_id,
                    e.fuel_type_id,
                    e.model_year_id,
                    e.pol_process_id,
                ),
                e,
            )
        })
        .collect();
    let mut day2: Vec<Tvg> = Vec::new();
    for t in &day {
        let key: Key7 = (
            t.hour_day_id,
            t.initial_hour_day_id,
            t.month_id,
            t.source_type_id,
            t.fuel_type_id,
            t.model_year_id,
            t.pol_process_id,
        );
        let (Some(hd), Some(ihd), Some(&e)) = (
            hour_day_of.get(&t.hour_day_id),
            hour_day_of.get(&t.initial_hour_day_id),
            weighted_by_key.get(&key),
        ) else {
            continue;
        };
        let Some(sum_i24) = window_sum(hd.day_id, ihd.hour_id, 24, t.initial_hour_day_id, e) else {
            continue; // inner join tvgSumI24
        };
        let xn =
            (1.0 - t.back_purge_factor) * sum_i24.min(t.average_canister_capacity) + t.tvg_sum_1h;
        day2.push(Tvg {
            soak_day_id: 2,
            xn,
            ..*t
        });
    }
    out.extend(day2.iter().copied());

    // Soak days 3.. — Xn = (1 − bp)·least(Xnₙ₋₁ + tvgSumH24, cap) + tvgSum1H.
    let mut later_soak_days: Vec<i32> = inputs
        .sample_vehicle_soaking
        .iter()
        .map(|s| s.soak_day_id)
        .filter(|&d| d > 2)
        .collect();
    later_soak_days.sort_unstable();
    later_soak_days.dedup();

    let mut prev = day2;
    for soak_day_id in later_soak_days {
        let current: Vec<Tvg> = prev
            .iter()
            .map(|t| {
                let xn = (1.0 - t.back_purge_factor)
                    * (t.xn + t.tvg_sum_h24).min(t.average_canister_capacity)
                    + t.tvg_sum_1h;
                Tvg {
                    soak_day_id,
                    xn,
                    ..*t
                }
            })
            .collect();
        out.extend(current.iter().copied());
        prev = current;
    }
    out
}

/// TVV-5 — calculate cumulative tank vapor vented (TVV).
///
/// Each `stmyTVVEquations` coefficient row is joined to the [`Tvg`] soak
/// recurrence on `(sourceType, modelYear, fuelType, polProcess)`, to
/// `AgeCategory` (the model year pinned to `year − ageID`) and to `HourDay`.
/// The cumulative vapour vented blends a non-leaking and a leaking-canister
/// term, each a **data-driven equation** (see [`VentingEquations`]):
///
/// ```text
/// TVV   = regClassFraction · greatest(0,
///           (1 − leakFraction)·tvvEq + leakFraction·leakEq)
/// TVV_IM = regClassFraction · greatest(0,
///           (1 − leakFracIM)·tvvEq + leakFracIM·leakEq)
/// ```
///
/// where `leakFracIM = coalesce(leakFractionIM, leakFraction)`. The two
/// equations are evaluated once per joined row against its [`EquationVars`].
/// `priorHourID` is the cyclic predecessor of `hourID` within `1..=24` —
/// `mod(hourID − 2 + 24, 24) + 1` — pre-computed for TVV-6's self-join.
fn cumulative_tank_vapor_vented(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
    equations: &dyn VentingEquations,
    tvg: &[Tvg],
) -> Vec<CumulativeTankVaporVented> {
    let mut tvg_by_key: HashMap<(i32, i32, i32, i32), Vec<&Tvg>> = HashMap::new();
    for t in tvg {
        tvg_by_key
            .entry((
                t.source_type_id,
                t.model_year_id,
                t.fuel_type_id,
                t.pol_process_id,
            ))
            .or_default()
            .push(t);
    }
    // AgeCategory.ageID is the table's primary key — one row per age.
    let age_exists: HashSet<i32> = inputs.age_category.iter().map(|a| a.age_id).collect();
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();

    let mut out = Vec::new();
    for coeffs in &inputs.stmy_tvv_equations {
        // INNER JOIN AgeCategory acat ON acat.ageID = year − modelYearID.
        let age_id = ctx.year - coeffs.model_year_id;
        if !age_exists.contains(&age_id) {
            continue;
        }
        let Some(tvg_rows) = tvg_by_key.get(&(
            coeffs.source_type_id,
            coeffs.model_year_id,
            coeffs.fuel_type_id,
            coeffs.pol_process_id,
        )) else {
            continue;
        };
        let leak_fraction_im = coeffs.leak_fraction_im.unwrap_or(coeffs.leak_fraction);
        for t in tvg_rows {
            let Some(hd) = hour_day_of.get(&t.hour_day_id) else {
                continue;
            };
            let vars = EquationVars {
                soak_day_id: t.soak_day_id,
                tvg_daily: t.tvg_daily,
                xn: t.xn,
                tvg_sum_1h: t.tvg_sum_1h,
                tvg_sum_h24: t.tvg_sum_h24,
                back_purge_factor: coeffs.back_purge_factor,
                average_canister_capacity: coeffs.average_canister_capacity,
                tank_size: coeffs.tank_size,
                tank_fill_fraction: coeffs.tank_fill_fraction,
            };
            let tvv_eq = equations.tvv(&coeffs.tvv_equation, &vars);
            let leak_eq = equations.leak(&coeffs.leak_equation, &vars);
            let fraction = coeffs.reg_class_fraction_of_source_type_model_year_fuel;
            let tank_vapor_vented = fraction
                * ((1.0 - coeffs.leak_fraction) * tvv_eq + coeffs.leak_fraction * leak_eq).max(0.0);
            let tank_vapor_vented_im = fraction
                * ((1.0 - leak_fraction_im) * tvv_eq + leak_fraction_im * leak_eq).max(0.0);
            // mod(hourID − 1 − 1 + 24, 24) + 1 — the cyclic prior hour.
            let prior_hour_id = (hd.hour_id - 2 + 24).rem_euclid(24) + 1;
            out.push(CumulativeTankVaporVented {
                soak_day_id: t.soak_day_id,
                reg_class_id: coeffs.reg_class_id,
                age_id,
                pol_process_id: coeffs.pol_process_id,
                day_id: hd.day_id,
                hour_id: hd.hour_id,
                initial_hour_day_id: t.initial_hour_day_id,
                month_id: t.month_id,
                source_type_id: t.source_type_id,
                fuel_type_id: t.fuel_type_id,
                tank_vapor_vented,
                tank_vapor_vented_im,
                hour_day_id: t.hour_day_id,
                prior_hour_id,
            });
        }
    }
    // Sorted on the SQL primary key for a canonical, reproducible order.
    out.sort_unstable_by_key(|c| {
        (
            c.soak_day_id,
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
/// "current" row it finds the "prior hour" row carrying the same `(soakDayID,
/// regClassID, ageID, polProcessID, initialHourDayID, monthID, sourceTypeID,
/// fuelTypeID, dayID)` and `hourID = priorHourID`. The hourly increment is
/// `greatest(currentTVV − priorTVV, 0)` — a `LEFT JOIN` miss contributes
/// `coalesce(priorTVV, 0) = 0` — **but is zeroed unless the cold-soak tank
/// temperature rose** from the prior hour to this one. A missing temperature
/// makes the `≤` comparison `NULL`, so the `CASE` falls through to the
/// difference (the multi-day script's addition over the single-day TVV-6).
fn unweighted_hourly_tvv(
    inputs: &MultidayTankVaporVentingInputs,
    cumulative: &[CumulativeTankVaporVented],
    zone_id: i32,
) -> Vec<UnweightedHourlyTvv> {
    // The full CummulativeTankVaporVented primary key.
    type CtvKey = (i32, i32, i32, i32, i32, i32, i32, i32, i32, i32);
    let key_of = |c: &CumulativeTankVaporVented, hour_id: i32| -> CtvKey {
        (
            c.soak_day_id,
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
    let temp_of: HashMap<(i32, i32), f64> = inputs
        .cold_soak_tank_temperature
        .iter()
        .map(|r| ((r.month_id, r.hour_id), r.cold_soak_tank_temperature))
        .collect();

    let mut out: Vec<UnweightedHourlyTvv> = cumulative
        .iter()
        .map(|ctv1| {
            let prior = by_key.get(&key_of(ctv1, ctv1.prior_hour_id));
            let prior_tvv = prior.map_or(0.0, |p| p.tank_vapor_vented);
            let prior_tvv_im = prior.map_or(0.0, |p| p.tank_vapor_vented_im);
            // The CASE zeroes the increment only when both temperatures are
            // present and the temperature did not rise.
            let this_temp = temp_of.get(&(ctv1.month_id, ctv1.hour_id));
            let prior_temp = temp_of.get(&(ctv1.month_id, ctv1.prior_hour_id));
            let not_warming = matches!((this_temp, prior_temp), (Some(t1), Some(t2)) if t1 <= t2);
            let (tvv, tvv_im) = if not_warming {
                (0.0, 0.0)
            } else {
                (
                    (ctv1.tank_vapor_vented - prior_tvv).max(0.0),
                    (ctv1.tank_vapor_vented_im - prior_tvv_im).max(0.0),
                )
            };
            UnweightedHourlyTvv {
                zone_id,
                soak_day_id: ctv1.soak_day_id,
                reg_class_id: ctv1.reg_class_id,
                age_id: ctv1.age_id,
                pol_process_id: ctv1.pol_process_id,
                hour_day_id: ctv1.hour_day_id,
                initial_hour_day_id: ctv1.initial_hour_day_id,
                month_id: ctv1.month_id,
                source_type_id: ctv1.source_type_id,
                fuel_type_id: ctv1.fuel_type_id,
                unweighted_hourly_tvv: tvv,
                unweighted_hourly_tvv_im: tvv_im,
            }
        })
        .collect();
    out.sort_unstable_by_key(|u| {
        (
            u.soak_day_id,
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

/// The six-column key TVV-7 weights `UnweightedHourlyTVV` by:
/// `(soakDayID, sourceTypeID, zoneID, monthID, hourDayID, initialHourDayID)`.
type CsihfKey = (i32, i32, i32, i32, i32, i32);

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
/// The multi-day script first folds the soak fractions of soaking days beyond
/// the first into `coldSoakInitialHourFraction`: each `sampleVehicleSoaking`
/// row with `soakDayID > 1` becomes a fraction row at `(hour, day)` with the
/// initial hour pinned to hour 1 of that day. The extracted
/// `coldSoakInitialHourFraction` rows are soaking day 1.
///
/// Part A then weights each [`UnweightedHourlyTvv`] row by its matching
/// `coldSoakInitialHourFraction` (the [`CsihfKey`] join) and sums across the
/// `soakDayID` and `initialHourDayID` dimensions, producing one row per
/// [`HourlyTvvKey`].
///
/// Part B handles the four post-peak decay hours: each part-A row sitting
/// *at* its month's peak hour seeds rows for hours `peak + 1 ..= peak + 4` on
/// the same `dayID`, scaled by the fixed decay schedule
/// `0.0200, 0.0100, 0.0040, 0.0005`. The SQL reads these from a
/// `CopyOfHourlyTVV` snapshot taken before part B's insert, so the decay rows
/// derive from part A alone.
fn hourly_tvv(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
    unweighted: &[UnweightedHourlyTvv],
    peak_hours: &[PeakHourOfColdSoak],
) -> Vec<HourlyTvv> {
    let hour_day_id_of: HashMap<(i32, i32), i32> = inputs
        .hour_day
        .iter()
        .map(|hd| ((hd.day_id, hd.hour_id), hd.hour_day_id))
        .collect();

    // The full coldSoakInitialHourFraction set: soaking day 1 is the extracted
    // table; soaking days > 1 are folded in from sampleVehicleSoaking.
    let mut csihf_of: HashMap<CsihfKey, f64> = HashMap::new();
    for r in &inputs.cold_soak_initial_hour_fraction {
        csihf_of.insert(
            (
                1,
                r.source_type_id,
                r.zone_id,
                r.month_id,
                r.hour_day_id,
                r.initial_hour_day_id,
            ),
            r.cold_soak_initial_hour_fraction,
        );
    }
    for s in &inputs.sample_vehicle_soaking {
        if s.soak_day_id <= 1 {
            continue;
        }
        let (Some(&hour_day_id), Some(&initial_hour_day_id)) = (
            hour_day_id_of.get(&(s.day_id, s.hour_id)),
            hour_day_id_of.get(&(s.day_id, 1)),
        ) else {
            continue;
        };
        csihf_of.insert(
            (
                s.soak_day_id,
                s.source_type_id,
                ctx.zone_id,
                ctx.month_id,
                hour_day_id,
                initial_hour_day_id,
            ),
            s.soak_fraction,
        );
    }

    // --- Part A: weight by cold-soak fraction, sum over soakDayID + initial ---
    let mut grouped: HashMap<HourlyTvvKey, (f64, f64)> = HashMap::new();
    for u in unweighted {
        let Some(&fraction) = csihf_of.get(&(
            u.soak_day_id,
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
    let peak_of: HashMap<i32, i32> = peak_hours
        .iter()
        .map(|p| (p.month_id, p.peak_hour_id))
        .collect();

    let mut part_b = Vec::new();
    for htvv in &part_a {
        let Some(hd) = hour_day_of.get(&htvv.hour_day_id) else {
            continue;
        };
        let Some(&peak) = peak_of.get(&htvv.month_id) else {
            continue;
        };
        if hd.hour_id != peak {
            continue;
        }
        for offset in 1..=4 {
            let Some(&hour_day_id) = hour_day_id_of.get(&(hd.day_id, peak + offset)) else {
                continue;
            };
            let scale = match offset {
                1 => 0.0200,
                2 => 0.0100,
                3 => 0.0040,
                _ => 0.0005,
            };
            part_b.push(HourlyTvv {
                hour_day_id,
                hourly_tvv: htvv.hourly_tvv * scale,
                hourly_tvv_im: htvv.hourly_tvv_im * scale,
                ..*htvv
            });
        }
    }

    let mut out = part_a;
    out.extend(part_b);
    out.sort_unstable_by_key(hourly_tvv_key);
    out
}

/// The RVP-interpolated cubic adjustment-polynomial terms for one fuel type —
/// `averageTankGasoline.adjustTerm3 / 2 / 1` and `adjustConstant`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct AdjustTerms {
    term3: f64,
    term2: f64,
    term1: f64,
    constant: f64,
}

/// Compute the RVP-interpolated `averageTankGasoline.adjustTerm*` columns —
/// the SQL block that rebuilds `averageTankGasoline` before TVV-8.
///
/// For each fuel type the SQL bounds `evapRVPTemperatureAdjustment` with
/// sentinel knots at `RVP = −1` (carrying the lowest real knot's terms) and
/// `RVP = 1000` (the highest real knot's terms), so every RVP falls inside
/// the knot range. Each `averageTankGasoline` row's terms are then linearly
/// interpolated between the knot below its `RVP` and the knot above:
///
/// ```text
/// term = lowKnot.term
///        + (highKnot.term − lowKnot.term) / (highKnot.RVP − lowKnot.RVP)
///          · (atg.RVP − lowKnot.RVP)
/// ```
///
/// The SQL truncates and refills `averageTankGasoline` from this join, so a
/// fuel type with **no** `evapRVPTemperatureAdjustment` knots drops out of
/// `averageTankGasoline` entirely — that extract is filtered to fuel types 1
/// and 5, so TVV-8's operating / hot-soak insert (which `INNER JOIN`s
/// `averageTankGasoline`) only ever emits those fuel types. `AverageTankGasoline`
/// is extracted to the run's single month, so this indexes one row per fuel
/// type.
fn rvp_adjustment_terms(inputs: &MultidayTankVaporVentingInputs) -> HashMap<i32, AdjustTerms> {
    let mut knots_by_fuel: HashMap<i32, Vec<&EvapRvpTemperatureAdjustmentRow>> = HashMap::new();
    for k in &inputs.evap_rvp_temperature_adjustment {
        knots_by_fuel.entry(k.fuel_type_id).or_default().push(k);
    }
    let mut atg_by_fuel: HashMap<i32, &AverageTankGasolineRow> = HashMap::new();
    for atg in &inputs.average_tank_gasoline {
        atg_by_fuel.entry(atg.fuel_type_id).or_insert(atg);
    }

    let mut out = HashMap::new();
    for (fuel_type_id, knots) in &knots_by_fuel {
        let Some(atg) = atg_by_fuel.get(fuel_type_id) else {
            continue;
        };
        // The knot set, extended with the −1 / 1000 sentinels carrying the
        // lowest / highest real knot's terms (the SQL `insert ignore`s).
        let Some(min_knot) = knots.iter().min_by(|a, b| a.rvp.total_cmp(&b.rvp)).copied() else {
            continue;
        };
        let max_knot = knots
            .iter()
            .max_by(|a, b| a.rvp.total_cmp(&b.rvp))
            .copied()
            .expect("non-empty knot list has a max");
        let mut all: Vec<(f64, AdjustTerms)> = knots
            .iter()
            .map(|k| {
                (
                    k.rvp,
                    AdjustTerms {
                        term3: k.adjust_term_3,
                        term2: k.adjust_term_2,
                        term1: k.adjust_term_1,
                        constant: k.adjust_constant,
                    },
                )
            })
            .collect();
        all.push((
            -1.0,
            AdjustTerms {
                term3: min_knot.adjust_term_3,
                term2: min_knot.adjust_term_2,
                term1: min_knot.adjust_term_1,
                constant: min_knot.adjust_constant,
            },
        ));
        all.push((
            1000.0,
            AdjustTerms {
                term3: max_knot.adjust_term_3,
                term2: max_knot.adjust_term_2,
                term1: max_knot.adjust_term_1,
                constant: max_knot.adjust_constant,
            },
        ));

        // lowAdj = the knot with the greatest RVP ≤ atg.RVP; highAdj = the
        // knot with the least RVP > atg.RVP.
        let low = all
            .iter()
            .filter(|(rvp, _)| *rvp <= atg.rvp)
            .max_by(|a, b| a.0.total_cmp(&b.0));
        let high = all
            .iter()
            .filter(|(rvp, _)| *rvp > atg.rvp)
            .min_by(|a, b| a.0.total_cmp(&b.0));
        let (Some(&(low_rvp, low)), Some(&(high_rvp, high))) = (low, high) else {
            continue;
        };
        let interp = |lo: f64, hi: f64| lo + (hi - lo) / (high_rvp - low_rvp) * (atg.rvp - low_rvp);
        out.insert(
            *fuel_type_id,
            AdjustTerms {
                term3: interp(low.term3, high.term3),
                term2: interp(low.term2, high.term2),
                term1: interp(low.term1, high.term1),
                constant: interp(low.constant, high.constant),
            },
        );
    }
    out
}

/// The eight-column `WeightedMeanBaseRate` primary / group key:
/// `(polProcessID, sourceTypeID, regClassID, fuelTypeID, monthID, hourDayID,
/// modelYearID, opModeID)`.
type WeightedRateKey = (i32, i32, i32, i32, i32, i32, i32, i32);

/// TVV-8 — calculate the I/M-adjusted mean base rates.
///
/// `WeightedMeanBaseRate` is filled by two independent inserts writing
/// disjoint operating modes:
///
/// * The **cold-soak** insert (`opModeID = 151`) carries the venting chain's
///   [`HourlyTvv`] forward, weighting `hourlyTVV` by `sourceBinActivityFraction`
///   and summing per `(polProcessID, sourceTypeID, regClassID, fuelTypeID,
///   monthID, hourDayID, modelYearID)`.
/// * The **operating / hot-soak** insert (`opModeID ∈ {150, 300}`) weights
///   `EmissionRateByAge.meanBaseRate` by `sourceBinActivityFraction`,
///   cross-joining `RunSpecHourDay` and joining `ZoneMonthHour` for the
///   ambient temperature, then multiplies the summed rate by the
///   **temperature / RVP adjustment** ([`rvp_adjustment_terms`]):
///
///   ```text
///   tempAdj = Σ etaTermₖ · max(temperature, 40)ᵏ
///   rvpAdj  = Σ atgTermₖ · temperatureᵏ   (temperature ≥ 40, else 1)
///   adjustment = tempAdj · rvpAdj   (operating mode 300, fuel 1 or 5; else 1)
///   ```
///
/// Both inserts keep only fuel types with `subjectToEvapCalculations = 'Y'`
/// and require a `PollutantProcessModelYear` row linking the source bin's
/// model-year group.
fn weighted_mean_base_rate(
    inputs: &MultidayTankVaporVentingInputs,
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
    let evap_ok = |fuel_type_id: i32| evap_fuel_type.get(&fuel_type_id) == Some(&true);
    let ppmy_links: HashSet<(i32, i32, i32)> = inputs
        .pollutant_process_model_year
        .iter()
        .map(|p| (p.pol_process_id, p.model_year_id, p.model_year_group_id))
        .collect();

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
                if !ppmy_links.contains(&(
                    sbd.pol_process_id,
                    stmy.model_year_id,
                    sb.model_year_group_id,
                )) {
                    continue;
                }
                let entry = grouped
                    .entry((
                        htvv.pol_process_id,
                        htvv.source_type_id,
                        sb.reg_class_id,
                        sb.fuel_type_id,
                        htvv.month_id,
                        htvv.hour_day_id,
                        stmy.model_year_id,
                        COLD_SOAK_OP_MODE_ID,
                    ))
                    .or_insert((0.0, 0.0));
                entry.0 += sbd.source_bin_activity_fraction * htvv.hourly_tvv;
                entry.1 += sbd.source_bin_activity_fraction * htvv.hourly_tvv_im;
            }
        }
    }
    let mut out: Vec<WeightedMeanBaseRate> = grouped
        .into_iter()
        .map(|(key, (rate, rate_im))| WeightedMeanBaseRate {
            pol_process_id: key.0,
            source_type_id: key.1,
            reg_class_id: key.2,
            fuel_type_id: key.3,
            month_id: key.4,
            hour_day_id: key.5,
            model_year_id: key.6,
            op_mode_id: key.7,
            weighted_mean_base_rate: rate,
            weighted_mean_base_rate_im: rate_im,
        })
        .collect();

    // --- Operating / hot-soak insert: opModeID ∈ {150, 300} ---
    out.extend(operating_mode_base_rates(
        inputs,
        ctx,
        &source_bin_of,
        &evap_ok,
        &ppmy_links,
    ));

    out.sort_unstable_by_key(|w| {
        (
            w.pol_process_id,
            w.source_type_id,
            w.reg_class_id,
            w.fuel_type_id,
            w.month_id,
            w.hour_day_id,
            w.model_year_id,
            w.op_mode_id,
        )
    });
    out
}

/// TVV-8's operating / hot-soak insert — the `EmissionRateByAge`-driven half
/// of [`weighted_mean_base_rate`], split out for readability.
fn operating_mode_base_rates(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
    source_bin_of: &HashMap<i64, &SourceBinRow>,
    evap_ok: &impl Fn(i32) -> bool,
    ppmy_links: &HashSet<(i32, i32, i32)>,
) -> Vec<WeightedMeanBaseRate> {
    // `evapTemperatureAdjustment` is cross-joined; the extract leaves one row.
    let Some(eta) = inputs.evap_temperature_adjustment.first() else {
        return Vec::new();
    };
    let rvp_terms = rvp_adjustment_terms(inputs);
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
    let hour_day_of: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let mut zmh_by_hour: HashMap<i32, Vec<&ZoneMonthHourRow>> = HashMap::new();
    for zmh in &inputs.zone_month_hour {
        zmh_by_hour.entry(zmh.hour_id).or_default().push(zmh);
    }

    // Accumulate the unadjusted rate sum and the per-group adjustment (the
    // adjustment is constant within a group — month, hourDay, fuel, opMode
    // are all group keys).
    let mut grouped: HashMap<WeightedRateKey, (f64, f64, f64)> = HashMap::new();
    for er in &inputs.emission_rate_by_age {
        let Some(sb) = source_bin_of.get(&er.source_bin_id) else {
            continue;
        };
        if !evap_ok(sb.fuel_type_id) {
            continue;
        }
        // `averageTankGasoline` only survives for fuel types with RVP knots.
        let Some(&rvp) = rvp_terms.get(&sb.fuel_type_id) else {
            continue;
        };
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
                if !ppmy_links.contains(&(
                    sbd.pol_process_id,
                    stmy.model_year_id,
                    sb.model_year_group_id,
                )) {
                    continue;
                }
                let contribution = sbd.source_bin_activity_fraction * er.mean_base_rate;
                let contribution_im = sbd.source_bin_activity_fraction * er.mean_base_rate_im;
                // Cross join RunSpecHourDay; ZoneMonthHour gives month + temp.
                for &hour_day_id in &inputs.run_spec_hour_day {
                    let Some(hd) = hour_day_of.get(&hour_day_id) else {
                        continue;
                    };
                    let Some(zmhs) = zmh_by_hour.get(&hd.hour_id) else {
                        continue;
                    };
                    for zmh in zmhs {
                        let adjustment = if er.op_mode_id == OPERATING_OP_MODE_ID
                            && (sb.fuel_type_id == 1 || sb.fuel_type_id == 5)
                        {
                            operating_adjustment(eta, &rvp, zmh.temperature)
                        } else {
                            1.0
                        };
                        let entry = grouped
                            .entry((
                                er.pol_process_id,
                                stmy.source_type_id,
                                sb.reg_class_id,
                                sb.fuel_type_id,
                                zmh.month_id,
                                hour_day_id,
                                stmy.model_year_id,
                                er.op_mode_id,
                            ))
                            .or_insert((0.0, 0.0, adjustment));
                        entry.0 += contribution;
                        entry.1 += contribution_im;
                        entry.2 = adjustment;
                    }
                }
            }
        }
    }
    grouped
        .into_iter()
        .map(|(key, (rate, rate_im, adjustment))| WeightedMeanBaseRate {
            pol_process_id: key.0,
            source_type_id: key.1,
            reg_class_id: key.2,
            fuel_type_id: key.3,
            month_id: key.4,
            hour_day_id: key.5,
            model_year_id: key.6,
            op_mode_id: key.7,
            weighted_mean_base_rate: rate * adjustment,
            weighted_mean_base_rate_im: rate_im * adjustment,
        })
        .collect()
}

/// The operating-mode temperature / RVP adjustment factor — TVV-8's
/// `tempAdjustment · rvpAdjustment` for operating mode 300 on fuel 1 / 5.
///
/// `tempAdjustment` is the cubic `evapTemperatureAdjustment` polynomial in
/// `max(temperature, 40)`; `rvpAdjustment` is the cubic RVP-interpolated
/// `averageTankGasoline.adjustTerm*` polynomial in `temperature`, but `1.0`
/// below 40 °F.
fn operating_adjustment(eta: &EvapTemperatureAdjustmentRow, rvp: &AdjustTerms, temp: f64) -> f64 {
    let t_floored = temp.max(40.0);
    let temp_adjustment = eta.temp_adjust_term_3 * t_floored.powi(3)
        + eta.temp_adjust_term_2 * t_floored.powi(2)
        + eta.temp_adjust_term_1 * t_floored
        + eta.temp_adjust_constant;
    let rvp_adjustment = if temp >= 40.0 {
        rvp.term3 * temp.powi(3) + rvp.term2 * temp.powi(2) + rvp.term1 * temp + rvp.constant
    } else {
        1.0
    };
    temp_adjustment * rvp_adjustment
}

/// TVV-9 — assemble the `MOVESWorkerOutput` rows.
///
/// Each [`WeightedMeanBaseRate`] row is joined to `SourceHours` (on
/// `hourDayID`, `monthID`, the derived `ageID = year − modelYearID`, and
/// `sourceTypeID`), to `OpModeDistribution` (on `sourceTypeID`, `hourDayID`,
/// `polProcessID` and `opModeID`), and through `PollutantProcessAssoc` and
/// `HourDay`. The emission is
///
/// ```text
/// emissionQuant   = weightedMeanBaseRate   · sourceHours · opModeFraction
/// emissionQuantIM = weightedMeanBaseRateIM · sourceHours · opModeFraction
/// ```
///
/// The closing `UPDATE` blends in the I/M adjustment for every row matching an
/// `IMCoverageMergedUngrouped` entry on `(processID, pollutantID, modelYearID,
/// fuelTypeID, sourceTypeID)` — `regClassID` is not part of that key. Rows
/// with no I/M coverage keep the plain `emissionQuant`. The multi-day output
/// carries `regClassID` (the single-day script does not).
fn assemble_emission_output(
    inputs: &MultidayTankVaporVentingInputs,
    ctx: &RunContext,
    weighted_rates: &[WeightedMeanBaseRate],
    im_merged: &[ImCoverageMerged],
) -> Vec<MultidayTankVaporVentingEmissionRow> {
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
                out.push(MultidayTankVaporVentingEmissionRow {
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
                    reg_class_id: w.reg_class_id,
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

/// The MOVES multi-day tank vapor venting calculator.
///
/// A zero-sized value type: it owns no per-run state and no master-loop
/// subscription — see the module documentation for why
/// [`Calculator::subscriptions`] / [`Calculator::registrations`] are empty.
/// All run-varying input flows through the [`MultidayTankVaporVentingInputs`]
/// / [`RunContext`] / [`VentingEquations`] arguments to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy)]
pub struct MultidayTankVaporVentingCalculator;

impl MultidayTankVaporVentingCalculator {
    /// The SQL script name this module ports. Not a `calculator-dag.json`
    /// entry — see the module documentation.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Compute the multi-day tank-vapor-venting emission rows — the port of
    /// the `MultidayTankVaporVentingCalculator.sql` "Processing" section.
    ///
    /// The nine numbered TVV steps run in order, with the TVG soak recurrence
    /// between TVV-4 and TVV-5: TVV-1 merges the I/M adjustment fractions,
    /// TVV-2 finds the peak cold-soak hour, TVV-3 generates altitude-
    /// interpolated tank vapour, TVV-4 ethanol-weights and differences it,
    /// the soak recurrence accumulates the canister load across soaking days,
    /// TVV-5 drives cumulative vapour vented from the `equations`, TVV-6
    /// differences it into hourly increments, TVV-7 cold-soak-weights it and
    /// adds the post-peak decay hours, TVV-8 builds the weighted mean base
    /// rates, and TVV-9 assembles the output. The result is sorted by its
    /// integer dimension columns (ties broken on `emissionQuant`) for
    /// deterministic output — MOVES leaves `MOVESWorkerOutput` unordered.
    #[must_use]
    pub fn calculate(
        &self,
        inputs: &MultidayTankVaporVentingInputs,
        ctx: &RunContext,
        equations: &dyn VentingEquations,
    ) -> Vec<MultidayTankVaporVentingEmissionRow> {
        let im_merged = im_coverage_merged_ungrouped(inputs, ctx);
        let peak_hours = peak_hour_of_cold_soak(inputs);
        let generated = tank_vapor_generated(inputs, ctx, &peak_hours);
        let weighted = ethanol_weighted_tvg(inputs, &generated);
        let tvg = tvg_soak_recurrence(inputs, &weighted);
        let cumulative = cumulative_tank_vapor_vented(inputs, ctx, equations, &tvg);
        let unweighted = unweighted_hourly_tvv(inputs, &cumulative, ctx.zone_id);
        let hourly = hourly_tvv(inputs, ctx, &unweighted, &peak_hours);
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

impl Default for MultidayTankVaporVentingCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-DB / execution-DB tables the multi-day venting computation
/// consumes — the [`MultidayTankVaporVentingInputs`] fields. `stmyTVVCoeffs`,
/// `stmyTVVEquations` and `sampleVehicleSoaking` are produced by the
/// `NewTVVYear` / `FillSampleVehicleSoaking` setup sections (see the module
/// documentation) and listed here as the Processing section's inputs.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "AverageTankGasoline",
    "ColdSoakInitialHourFraction",
    "ColdSoakTankTemperature",
    "County",
    "EmissionRateByAge",
    "evapTemperatureAdjustment",
    "evapRVPTemperatureAdjustment",
    "FuelType",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "MonthOfAnyYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessModelYear",
    "RunSpecHourDay",
    "RunSpecSourceType",
    "sampleVehicleSoaking",
    "SourceBin",
    "SourceBinDistribution",
    "SourceHours",
    "SourceTypeModelYear",
    "stmyTVVCoeffs",
    "stmyTVVEquations",
    "TankVaporGenCoeffs",
    "ZoneMonthHour",
];

/// Fallback `VentingEquations` for use in `execute()`: returns `0.0` for
/// every equation, matching the Java's `CASE … ELSE 0 END` behaviour for
/// equation names that are not recognised.
struct ZeroEquations;
impl VentingEquations for ZeroEquations {
    fn tvv(&self, _: &str, _: &EquationVars) -> f64 {
        0.0
    }
    fn leak(&self, _: &str, _: &EquationVars) -> f64 {
        0.0
    }
}

impl Calculator for MultidayTankVaporVentingCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Empty — `MultidayTankVaporVentingCalculator` is the multi-day SQL
    /// script for the `TankVaporVentingCalculator` calculator, not a distinct
    /// calculator. The live `Subscribe` directive (process 12 at `MONTH`)
    /// belongs to [`crate::calculators::tank_vapor_venting_calculator`]; see
    /// the module documentation.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &[]
    }

    /// Empty — the live `Registration` directive (THC × process 12) belongs
    /// to [`crate::calculators::tank_vapor_venting_calculator`]; returning it
    /// here too would double-register the pair. See the module documentation.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &[]
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        // Prefer the fuelUsageFraction-remapped distribution from scratch (written
        // by SourceBinDistributionGenerator) over the raw slow-tier table.
        let fuel_usage_table = {
            let process_id_i64 = pos.process_id.map(|p| i64::from(p.0)).unwrap_or(0);
            let county_id_i64 = pos.location.county_id.map(i64::from).unwrap_or(0);
            let year_i64 = pos.time.year.map(i64::from).unwrap_or(0);
            format!("sourceBinDistributionFuelUsage_{process_id_i64}_{county_id_i64}_{year_i64}")
        };
        let run_ctx = RunContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            month_id: pos.time.month.map(|m| m as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            road_type_id: 0, // road_type_id not yet exposed in ExecutionLocation
        };
        let inputs = MultidayTankVaporVentingInputs {
            age_category: tables.iter_typed::<AgeCategoryRow>("AgeCategory")?,
            average_tank_gasoline: tables
                .iter_typed::<AverageTankGasolineRow>("AverageTankGasoline")?,
            cold_soak_initial_hour_fraction: tables
                .iter_typed::<ColdSoakInitialHourFractionRow>("ColdSoakInitialHourFraction")?,
            cold_soak_tank_temperature: tables
                .iter_typed::<ColdSoakTankTemperatureRow>("ColdSoakTankTemperature")?,
            county: tables.iter_typed::<CountyRow>("County")?,
            emission_rate_by_age: tables.iter_typed::<EmissionRateByAgeRow>("EmissionRateByAge")?,
            evap_temperature_adjustment: tables
                .iter_typed::<EvapTemperatureAdjustmentRow>("evapTemperatureAdjustment")?,
            evap_rvp_temperature_adjustment: tables
                .iter_typed::<EvapRvpTemperatureAdjustmentRow>("evapRVPTemperatureAdjustment")?,
            fuel_type: tables.iter_typed::<FuelTypeRow>("FuelType")?,
            hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
            im_coverage: tables.iter_typed::<ImCoverageRow>("IMCoverage")?,
            im_factor: tables.iter_typed::<ImFactorRow>("IMFactor")?,
            month_of_any_year: tables.iter_typed::<MonthOfAnyYearRow>("MonthOfAnyYear")?,
            op_mode_distribution: tables
                .iter_typed::<OpModeDistributionRow>("OpModeDistribution")?,
            pollutant_process_assoc: tables
                .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
            pollutant_process_model_year: tables
                .iter_typed::<PollutantProcessModelYearRow>("PollutantProcessModelYear")?,
            run_spec_hour_day: tables
                .iter_typed::<RunSpecHourDayIdRow>("RunSpecHourDay")?
                .into_iter()
                .map(|r| r.hour_day_id)
                .collect(),
            run_spec_source_type: tables
                .iter_typed::<RunSpecSourceTypeIdRow>("RunSpecSourceType")?
                .into_iter()
                .map(|r| r.source_type_id)
                .collect(),
            sample_vehicle_soaking: tables
                .iter_typed::<SampleVehicleSoakingRow>("sampleVehicleSoaking")?,
            source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
            source_bin_distribution: {
                let scratch = ctx.scratch();
                if scratch.store.contains(&fuel_usage_table) {
                    scratch
                        .store
                        .iter_typed::<SourceBinDistributionRow>(&fuel_usage_table)?
                } else {
                    tables.iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?
                }
            },
            source_hours: tables.iter_typed::<SourceHoursRow>("SourceHours")?,
            source_type_model_year: tables
                .iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
            stmy_tvv_coeffs: tables.iter_typed::<StmyTvvCoeffsRow>("stmyTVVCoeffs")?,
            stmy_tvv_equations: tables.iter_typed::<StmyTvvEquationsRow>("stmyTVVEquations")?,
            tank_vapor_gen_coeffs: tables
                .iter_typed::<TankVaporGenCoeffsRow>("TankVaporGenCoeffs")?,
            zone_month_hour: tables.iter_typed::<ZoneMonthHourRow>("ZoneMonthHour")?,
        };
        let rows = self.calculate(&inputs, &run_ctx, &ZeroEquations);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(MultidayTankVaporVentingCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run context the fixtures use: calendar year 2020, month 7,
    /// county 26161 of state 26, zone 90, link 5001, road type 5.
    fn run_context() -> RunContext {
        RunContext {
            year: 2020,
            state_id: 26,
            county_id: 26_161,
            zone_id: 90,
            link_id: 5001,
            road_type_id: 5,
            month_id: 7,
        }
    }

    /// A test [`VentingEquations`]: the `tvvEquation` named `"T0"` returns the
    /// running canister load `Xn` scaled by the wrapped factor, the
    /// `leakEquation` returns `0`.
    struct ScaledXn(f64);

    impl VentingEquations for ScaledXn {
        fn tvv(&self, equation: &str, vars: &EquationVars) -> f64 {
            match equation {
                "T0" => vars.xn * self.0,
                _ => 0.0,
            }
        }

        fn leak(&self, _equation: &str, _vars: &EquationVars) -> f64 {
            0.0
        }
    }

    /// Run the calculator over `inputs` with the standard [`run_context`] and
    /// the identity [`ScaledXn`] equations.
    fn run(inputs: &MultidayTankVaporVentingInputs) -> Vec<MultidayTankVaporVentingEmissionRow> {
        MultidayTankVaporVentingCalculator::new().calculate(inputs, &run_context(), &ScaledXn(1.0))
    }

    /// Assert two `emissionQuant`s match within `f64` slack.
    fn assert_quant(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "emissionQuant {actual} != expected {expected}",
        );
    }

    /// `D` — the temperature term `exp(0.01·70) − exp(0.01·60)` the minimal
    /// fixture's TVG reduces to (its `tvgTermB` is zero, so the RVP
    /// exponential is `exp(0) = 1`).
    fn temperature_term() -> f64 {
        (0.01_f64 * 70.0).exp() - (0.01_f64 * 60.0).exp()
    }

    /// A minimal one-of-everything input that threads exactly one row through
    /// the cold-soak (`opModeID = 151`) venting path.
    ///
    /// Hand-computed: TVV-3's `tankSize·(1 − tankFillFraction) = 5` prefactor
    /// gives `TVG = 5·D` at low altitude; the county sits at the low-altitude
    /// barometric pressure, so the interpolation keeps `5·D`. `ETOHVolume` 10
    /// weights TVV-4 entirely to ethanol level 10; the hour-1 self-pair makes
    /// the prior-hour increment `0`, so `ethanolWeightedTVG = 5·D`. The soak
    /// recurrence's day-1 `Xn = tvgSumIH = 5·D`; the `"T0"` equation returns
    /// `Xn`, `leakFraction = 0`, `regClassFraction = 1`, so `TVV = 5·D`.
    /// `sampleVehicleSoaking` is empty, so soaking day 2 finds no
    /// `coldSoakInitialHourFraction` and drops. TVV-6 differences against the
    /// (zero) hour-1 row to `5·D`, TVV-7 weights by the cold-soak fraction 1,
    /// TVV-8 by `sourceBinActivityFraction` 1, TVV-9 by `sourceHours` 10 ×
    /// `opModeFraction` 1 — `emissionQuant = 50·D`.
    fn minimal_inputs() -> MultidayTankVaporVentingInputs {
        MultidayTankVaporVentingInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 300,
            }],
            average_tank_gasoline: vec![AverageTankGasolineRow {
                fuel_type_id: 1,
                month_group_id: 7,
                etoh_volume: 10.0,
                rvp: 9.0,
            }],
            cold_soak_initial_hour_fraction: vec![
                ColdSoakInitialHourFractionRow {
                    source_type_id: 21,
                    zone_id: 90,
                    month_id: 7,
                    hour_day_id: 25,
                    initial_hour_day_id: 15,
                    cold_soak_initial_hour_fraction: 1.0,
                },
                ColdSoakInitialHourFractionRow {
                    source_type_id: 21,
                    zone_id: 90,
                    month_id: 7,
                    hour_day_id: 15,
                    initial_hour_day_id: 15,
                    cold_soak_initial_hour_fraction: 1.0,
                },
            ],
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
                barometric_pressure: LOW_ALTITUDE_PRESSURE,
            }],
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
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 7,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 25,
                link_id: 5001,
                pol_process_id: 112,
                op_mode_id: COLD_SOAK_OP_MODE_ID,
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
            stmy_tvv_coeffs: vec![StmyTvvCoeffsRow {
                source_type_id: 21,
                model_year_id: 2018,
                fuel_type_id: 1,
                pol_process_id: 112,
                back_purge_factor: 0.0,
                average_canister_capacity: 1.0e9,
                leak_fraction: 0.0,
                leak_fraction_im: 0.0,
                tank_size: 10.0,
                tank_fill_fraction: 0.5,
            }],
            stmy_tvv_equations: vec![StmyTvvEquationsRow {
                source_type_id: 21,
                model_year_id: 2018,
                fuel_type_id: 1,
                pol_process_id: 112,
                reg_class_id: 30,
                back_purge_factor: 0.0,
                average_canister_capacity: 1.0e9,
                reg_class_fraction_of_source_type_model_year_fuel: 1.0,
                tvv_equation: "T0".to_string(),
                leak_equation: "L0".to_string(),
                leak_fraction: 0.0,
                leak_fraction_im: None,
                tank_size: 10.0,
                tank_fill_fraction: 0.5,
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
                TankVaporGenCoeffsRow {
                    ethanol_level_id: 0,
                    altitude: 'H',
                    tvg_term_a: 1.0,
                    tvg_term_b: 0.0,
                    tvg_term_c: 0.01,
                },
                TankVaporGenCoeffsRow {
                    ethanol_level_id: 10,
                    altitude: 'H',
                    tvg_term_a: 1.0,
                    tvg_term_b: 0.0,
                    tvg_term_c: 0.01,
                },
            ],
            ..MultidayTankVaporVentingInputs::default()
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
        assert_eq!(r.reg_class_id, 30); // multi-day output carries regClassID
        assert_eq!(r.fuel_type_id, 1);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 5);
        assert_quant(r.emission_quant, 50.0 * temperature_term());
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        let inputs = MultidayTankVaporVentingInputs::default();
        assert!(MultidayTankVaporVentingCalculator::new()
            .calculate(&inputs, &run_context(), &ScaledXn(1.0))
            .is_empty());
    }

    #[test]
    fn calculate_drives_cumulative_tvv_from_the_equation_evaluator() {
        // Doubling the `tvvEquation` result doubles the venting emission:
        // the equation evaluator, not the port, owns the cumulative-TVV
        // formula (50·D → 100·D).
        let inputs = minimal_inputs();
        let rows = MultidayTankVaporVentingCalculator::new().calculate(
            &inputs,
            &run_context(),
            &ScaledXn(2.0),
        );
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 100.0 * temperature_term());
    }

    #[test]
    fn calculate_blends_in_the_im_adjustment() {
        // An I/M program covering model year 2018: IMFactor 1.0 ×
        // complianceFactor 50 × 0.01 = 0.5 adjustment fraction. The chain runs
        // the base and I/M paths identically here (leakFraction 0,
        // meanBaseRate path unused), so emissionQuantIM = emissionQuant and
        // the blend leaves the result unchanged — the test pins that the I/M
        // UPDATE matches and does not corrupt the value.
        let mut inputs = minimal_inputs();
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
        assert_quant(rows[0].emission_quant, 50.0 * temperature_term());
    }

    #[test]
    fn calculate_interpolates_tvg_by_barometric_pressure() {
        // Put the county at the high-altitude pressure (interpolation factor
        // 1) and double the high-altitude `tvgTermA`: TVG follows the high
        // coefficients, 5·D → 10·D, so emissionQuant doubles to 100·D.
        let mut inputs = minimal_inputs();
        inputs.county[0].barometric_pressure = HIGH_ALTITUDE_PRESSURE;
        for c in &mut inputs.tank_vapor_gen_coeffs {
            if c.altitude == 'H' {
                c.tvg_term_a = 2.0;
            }
        }
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 100.0 * temperature_term());
    }

    #[test]
    fn calculate_drops_fuel_type_not_subject_to_evap() {
        let mut inputs = minimal_inputs();
        inputs.fuel_type[0].subject_to_evap_calculations = false;
        assert!(run(&inputs).is_empty());
    }

    #[test]
    fn peak_hour_of_cold_soak_takes_the_warmest_hour_breaking_ties_low() {
        let inputs = MultidayTankVaporVentingInputs {
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
            ],
            ..MultidayTankVaporVentingInputs::default()
        };
        let peaks = peak_hour_of_cold_soak(&inputs);
        assert_eq!(peaks.len(), 1);
        assert_eq!(peaks[0].month_id, 7);
        // Hours 2 and 3 tie at 70.0 °F; the earlier hour wins.
        assert_eq!(peaks[0].peak_hour_id, 2);
    }

    /// HourDay rows for hours 1 and 2 of day 5 — `hourDayID = hourID·10 + dayID`.
    fn hour_days_1_2() -> Vec<HourDayRow> {
        vec![
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
        ]
    }

    /// One [`EthanolWeightedTvg`] row at `hourDayID` / `initialHourDayID`.
    fn ewtvg(hour_day_id: i32, initial_hour_day_id: i32, value: f64) -> EthanolWeightedTvg {
        EthanolWeightedTvg {
            hour_day_id,
            initial_hour_day_id,
            month_id: 7,
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id: 2018,
            pol_process_id: 112,
            ethanol_weighted_tvg: value,
            back_purge_factor: 0.0,
            average_canister_capacity: 1.0e9,
        }
    }

    #[test]
    fn tvg_soak_recurrence_accumulates_xn_across_soaking_days() {
        // Hour 2 carries 5.0 of hourly TVG, hour 1 carries 0.0.
        let inputs = MultidayTankVaporVentingInputs {
            hour_day: hour_days_1_2(),
            // soakDayID 3 present → the recurrence iterates day 3.
            sample_vehicle_soaking: vec![SampleVehicleSoakingRow {
                soak_day_id: 3,
                source_type_id: 21,
                day_id: 5,
                hour_id: 1,
                soak_fraction: 1.0,
            }],
            ..MultidayTankVaporVentingInputs::default()
        };
        let weighted = vec![ewtvg(25, 15, 5.0), ewtvg(15, 15, 0.0)];
        let tvg = tvg_soak_recurrence(&inputs, &weighted);
        let at = |soak: i32, hour_day: i32| {
            *tvg.iter()
                .find(|t| t.soak_day_id == soak && t.hour_day_id == hour_day)
                .expect("TVG row")
        };
        // Day 1: Xn = TVGdaily = tvgSumIH (hours 1..H for hourDay 25 = 0 + 5).
        assert_quant(at(1, 25).xn, 5.0);
        assert_quant(at(1, 25).tvg_daily, 5.0);
        // Day 2: Xn = (1 − 0)·least(tvgSumI24 5, cap) + tvgSum1H 5 = 10.
        assert_quant(at(2, 25).xn, 10.0);
        // Day 3: Xn = least(Xn₂ 10 + tvgSumH24 0, cap) + tvgSum1H 5 = 15.
        assert_quant(at(3, 25).xn, 15.0);
        // TVGdaily is carried forward from day 1 unchanged.
        assert_quant(at(3, 25).tvg_daily, 5.0);
    }

    #[test]
    fn cumulative_tank_vapor_vented_blends_the_leaking_term() {
        // tvvEquation → Xn (8.0), leakEquation → 100.0; leakFraction 0.25,
        // regClassFraction 0.5:
        // TVV = 0.5 · ((1 − 0.25)·8 + 0.25·100) = 0.5 · 31 = 15.5.
        struct LeakProbe;
        impl VentingEquations for LeakProbe {
            fn tvv(&self, _equation: &str, vars: &EquationVars) -> f64 {
                vars.xn
            }
            fn leak(&self, _equation: &str, _vars: &EquationVars) -> f64 {
                100.0
            }
        }
        let inputs = MultidayTankVaporVentingInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 300,
            }],
            hour_day: hour_days_1_2(),
            stmy_tvv_equations: vec![StmyTvvEquationsRow {
                source_type_id: 21,
                model_year_id: 2018,
                fuel_type_id: 1,
                pol_process_id: 112,
                reg_class_id: 30,
                back_purge_factor: 0.0,
                average_canister_capacity: 1.0e9,
                reg_class_fraction_of_source_type_model_year_fuel: 0.5,
                tvv_equation: "T0".to_string(),
                leak_equation: "L0".to_string(),
                leak_fraction: 0.25,
                leak_fraction_im: None,
                tank_size: 10.0,
                tank_fill_fraction: 0.5,
            }],
            ..MultidayTankVaporVentingInputs::default()
        };
        let tvg = vec![Tvg {
            soak_day_id: 1,
            hour_day_id: 25,
            initial_hour_day_id: 15,
            month_id: 7,
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id: 2018,
            pol_process_id: 112,
            tvg_daily: 8.0,
            xn: 8.0,
            back_purge_factor: 0.0,
            average_canister_capacity: 1.0e9,
            tvg_sum_1h: 8.0,
            tvg_sum_h24: 0.0,
        }];
        let out = cumulative_tank_vapor_vented(&inputs, &run_context(), &LeakProbe, &tvg);
        assert_eq!(out.len(), 1);
        assert_quant(out[0].tank_vapor_vented, 15.5);
        // leakFractionIM falls back to leakFraction, so the I/M term matches.
        assert_quant(out[0].tank_vapor_vented_im, 15.5);
        assert_eq!(out[0].age_id, 2); // year 2020 − modelYear 2018
        assert_eq!(out[0].prior_hour_id, 1); // hour 2 → cyclic prior hour 1
    }

    #[test]
    fn unweighted_hourly_tvv_zeroes_when_the_tank_is_not_warming() {
        let row = |hour_id, prior_hour_id, tvv| CumulativeTankVaporVented {
            soak_day_id: 1,
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
        let cumulative = vec![row(1, 24, 2.0), row(2, 1, 5.0)];
        // Hour 2 is warmer than hour 1 — the increment is kept.
        let warming = MultidayTankVaporVentingInputs {
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
            ..MultidayTankVaporVentingInputs::default()
        };
        let out = unweighted_hourly_tvv(&warming, &cumulative, 90);
        let hour2 = out.iter().find(|u| u.hour_day_id == 25).unwrap();
        assert_quant(hour2.unweighted_hourly_tvv, 3.0); // 5 − 2
        assert_eq!(hour2.zone_id, 90);
        // Hour 2 is now cooler than hour 1 — the increment is zeroed.
        let mut cooling = warming;
        cooling.cold_soak_tank_temperature[1].cold_soak_tank_temperature = 50.0;
        let out = unweighted_hourly_tvv(&cooling, &cumulative, 90);
        let hour2 = out.iter().find(|u| u.hour_day_id == 25).unwrap();
        assert_quant(hour2.unweighted_hourly_tvv, 0.0);
    }

    #[test]
    fn calculator_name_matches_the_sql_script() {
        assert_eq!(
            MultidayTankVaporVentingCalculator::new().name(),
            "MultidayTankVaporVentingCalculator",
        );
        assert_eq!(
            MultidayTankVaporVentingCalculator::NAME,
            "MultidayTankVaporVentingCalculator",
        );
    }

    #[test]
    fn calculator_has_empty_registrations_and_subscriptions() {
        // The TankVaporVentingCalculator calculator (single-day module) owns
        // the live Registration / Subscribe directives; this multi-day script
        // module returns empty slices to avoid double-registering them.
        let calc = MultidayTankVaporVentingCalculator::new();
        assert!(calc.registrations().is_empty());
        assert!(calc.subscriptions().is_empty());
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn calculator_declares_input_tables() {
        let calc = MultidayTankVaporVentingCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "stmyTVVCoeffs",
            "stmyTVVEquations",
            "sampleVehicleSoaking",
            "TankVaporGenCoeffs",
            "evapRVPTemperatureAdjustment",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        // Use store.insert(...) directly to bypass schema-registry validation:
        // the multiday-TVV-specific column subsets differ from the registry canonical schemas.
        store.insert(
            "AgeCategory",
            AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap(),
        );
        store.insert(
            "AverageTankGasoline",
            AverageTankGasolineRow::into_dataframe(inputs.average_tank_gasoline.clone()).unwrap(),
        );
        store.insert(
            "ColdSoakInitialHourFraction",
            ColdSoakInitialHourFractionRow::into_dataframe(
                inputs.cold_soak_initial_hour_fraction.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "ColdSoakTankTemperature",
            ColdSoakTankTemperatureRow::into_dataframe(inputs.cold_soak_tank_temperature.clone())
                .unwrap(),
        );
        store.insert(
            "County",
            CountyRow::into_dataframe(inputs.county.clone()).unwrap(),
        );
        store.insert(
            "EmissionRateByAge",
            EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age.clone()).unwrap(),
        );
        store.insert(
            "evapTemperatureAdjustment",
            EvapTemperatureAdjustmentRow::into_dataframe(
                inputs.evap_temperature_adjustment.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "evapRVPTemperatureAdjustment",
            EvapRvpTemperatureAdjustmentRow::into_dataframe(
                inputs.evap_rvp_temperature_adjustment.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "FuelType",
            FuelTypeRow::into_dataframe(inputs.fuel_type.clone()).unwrap(),
        );
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day.clone()).unwrap(),
        );
        store.insert(
            "IMCoverage",
            ImCoverageRow::into_dataframe(inputs.im_coverage.clone()).unwrap(),
        );
        store.insert(
            "IMFactor",
            ImFactorRow::into_dataframe(inputs.im_factor.clone()).unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            MonthOfAnyYearRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap(),
        );
        store.insert(
            "OpModeDistribution",
            OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution.clone()).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc.clone())
                .unwrap(),
        );
        store.insert(
            "PollutantProcessModelYear",
            PollutantProcessModelYearRow::into_dataframe(
                inputs.pollutant_process_model_year.clone(),
            )
            .unwrap(),
        );
        // RunSpec* tables use thin wrapper types
        store.insert(
            "RunSpecHourDay",
            RunSpecHourDayIdRow::into_dataframe(
                inputs
                    .run_spec_hour_day
                    .iter()
                    .map(|&id| RunSpecHourDayIdRow { hour_day_id: id })
                    .collect(),
            )
            .unwrap(),
        );
        store.insert(
            "RunSpecSourceType",
            RunSpecSourceTypeIdRow::into_dataframe(
                inputs
                    .run_spec_source_type
                    .iter()
                    .map(|&id| RunSpecSourceTypeIdRow { source_type_id: id })
                    .collect(),
            )
            .unwrap(),
        );
        store.insert(
            "sampleVehicleSoaking",
            SampleVehicleSoakingRow::into_dataframe(inputs.sample_vehicle_soaking.clone()).unwrap(),
        );
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin.clone()).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution.clone())
                .unwrap(),
        );
        store.insert(
            "SourceHours",
            SourceHoursRow::into_dataframe(inputs.source_hours.clone()).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap(),
        );
        store.insert(
            "stmyTVVCoeffs",
            StmyTvvCoeffsRow::into_dataframe(inputs.stmy_tvv_coeffs.clone()).unwrap(),
        );
        store.insert(
            "stmyTVVEquations",
            StmyTvvEquationsRow::into_dataframe(inputs.stmy_tvv_equations.clone()).unwrap(),
        );
        store.insert(
            "TankVaporGenCoeffs",
            TankVaporGenCoeffsRow::into_dataframe(inputs.tank_vapor_gen_coeffs.clone()).unwrap(),
        );
        store.insert(
            "ZoneMonthHour",
            ZoneMonthHourRow::into_dataframe(inputs.zone_month_hour.clone()).unwrap(),
        );

        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = MultidayTankVaporVentingCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(
            out.dataframe().unwrap().height() > 0,
            "expected at least one row"
        );
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "MultidayTankVaporVentingCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calc: Box<dyn Calculator> = Box::new(MultidayTankVaporVentingCalculator::new());
        assert_eq!(calc.name(), "MultidayTankVaporVentingCalculator");
        assert!(calc.registrations().is_empty());
    }

    #[test]
    fn operating_adjustment_applies_the_temperature_floor_and_rvp_cutoff() {
        // Constant-only polynomials: tempAdjustConstant 2, RVP constant 3.
        let eta = EvapTemperatureAdjustmentRow {
            temp_adjust_term_3: 0.0,
            temp_adjust_term_2: 0.0,
            temp_adjust_term_1: 0.0,
            temp_adjust_constant: 2.0,
        };
        let rvp = AdjustTerms {
            term3: 0.0,
            term2: 0.0,
            term1: 0.0,
            constant: 3.0,
        };
        // Temperature 75 ≥ 40: tempAdj 2 · rvpAdj 3 = 6.
        assert_quant(operating_adjustment(&eta, &rvp, 75.0), 6.0);
        // Temperature 30 < 40: rvpAdj falls back to 1, leaving tempAdj 2.
        assert_quant(operating_adjustment(&eta, &rvp, 30.0), 2.0);
    }

    #[test]
    fn rvp_adjustment_terms_interpolates_between_the_knots() {
        // Knots at RVP 8 (constant 100) and RVP 12 (constant 200); the fuel's
        // RVP 10 sits halfway, so the interpolated constant is 150.
        let knot = |rvp, constant| EvapRvpTemperatureAdjustmentRow {
            fuel_type_id: 1,
            rvp,
            adjust_term_3: 0.0,
            adjust_term_2: 0.0,
            adjust_term_1: 0.0,
            adjust_constant: constant,
        };
        let inputs = MultidayTankVaporVentingInputs {
            average_tank_gasoline: vec![AverageTankGasolineRow {
                fuel_type_id: 1,
                month_group_id: 7,
                etoh_volume: 10.0,
                rvp: 10.0,
            }],
            evap_rvp_temperature_adjustment: vec![knot(8.0, 100.0), knot(12.0, 200.0)],
            ..MultidayTankVaporVentingInputs::default()
        };
        let terms = rvp_adjustment_terms(&inputs);
        assert_quant(terms[&1].constant, 150.0);
    }

    #[test]
    fn calculate_applies_the_operating_mode_temperature_rvp_adjustment() {
        // Operating-mode (300) path, no venting chain (`coldSoakInitialHour
        // Fraction` empty). meanBaseRate 4 weighted by sourceBinActivity 1,
        // then by the adjustment tempAdj 2 · rvpAdj 3 = 6 → 24; TVV-9 scales
        // by sourceHours 10 × opModeFraction 1 → emissionQuant 240.
        let inputs = MultidayTankVaporVentingInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 300,
            }],
            average_tank_gasoline: vec![AverageTankGasolineRow {
                fuel_type_id: 1,
                month_group_id: 7,
                etoh_volume: 10.0,
                rvp: 9.0,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 500_000,
                pol_process_id: 112,
                op_mode_id: OPERATING_OP_MODE_ID,
                age_group_id: 300,
                mean_base_rate: 4.0,
                mean_base_rate_im: 4.0,
            }],
            evap_temperature_adjustment: vec![EvapTemperatureAdjustmentRow {
                temp_adjust_term_3: 0.0,
                temp_adjust_term_2: 0.0,
                temp_adjust_term_1: 0.0,
                temp_adjust_constant: 2.0,
            }],
            evap_rvp_temperature_adjustment: vec![EvapRvpTemperatureAdjustmentRow {
                fuel_type_id: 1,
                rvp: 9.0,
                adjust_term_3: 0.0,
                adjust_term_2: 0.0,
                adjust_term_1: 0.0,
                adjust_constant: 3.0,
            }],
            fuel_type: vec![FuelTypeRow {
                fuel_type_id: 1,
                subject_to_evap_calculations: true,
            }],
            hour_day: hour_days_1_2(),
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 25,
                link_id: 5001,
                pol_process_id: 112,
                op_mode_id: OPERATING_OP_MODE_ID,
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
            zone_month_hour: vec![
                ZoneMonthHourRow {
                    month_id: 7,
                    hour_id: 1,
                    temperature: 75.0,
                },
                ZoneMonthHourRow {
                    month_id: 7,
                    hour_id: 2,
                    temperature: 75.0,
                },
            ],
            ..MultidayTankVaporVentingInputs::default()
        };
        let rows = run(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].process_id, 12);
        assert_eq!(rows[0].reg_class_id, 30);
        assert_quant(rows[0].emission_quant, 240.0);
    }
}
