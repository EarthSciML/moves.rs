//! Port of `CriteriaStartCalculator.java` and
//! `database/CriteriaStartCalculator.sql` — .
//!
//! `CriteriaStartCalculator` is the legacy scripted-SQL calculator for the
//! **criteria-pollutant start-exhaust** emission inventory: total gaseous
//! hydrocarbons (THC, pollutant 1), carbon monoxide (CO, pollutant 2) and
//! oxides of nitrogen (NOx, pollutant 3), all on the Start Exhaust process
//! (process 2). The Java constructor's capability set is the three
//! `polProcessID`s `102`/`202`/`302`.
//!
//! # Superseded by `BaseRateCalculator`
//!
//! This calculator is **not wired into the pinned MOVES runtime**.
//! `CalculatorInfo.txt` — the runtime registration file — has no
//! `Registration` directive for `CriteriaStartCalculator`, and
//! `characterization/calculator-chains/calculator-dag.json` records
//! `registrations_count: 0` to match. `CriteriaStartCalculator` is a
//! `GenericCalculatorBase` subclass with no Go worker: the base-rate
//! approach (, `BaseRateCalculator`) superseded the
//! older per-pollutant scripted-SQL calculators like this one, and the
//! THC/CO/NOx start-exhaust `(pollutant, process)` pairs are registered to
//! `BaseRateCalculator` instead.
//!
//! The still lists the class as a task, so this
//! module ports the **algorithm** — the SQL's `-- Section Processing` — for
//! reference and for cross-validation against `BaseRateCalculator`. To stay
//! consistent with the runtime, [`Calculator::registrations`] returns an
//! empty slice; the registry must not double-register the start-exhaust
//! pairs.
//!
//! # What it computes
//!
//! The criteria start-exhaust emission inventory is engine-start activity
//! multiplied by a temperature-adjusted, fuel-adjusted, source-bin- and
//! operating-mode-weighted emission rate, with an inspection-and-maintenance
//! (I/M) blend applied last:
//!
//! ```text
//! emissionQuant = meanBaseRate × starts
//! emissionQuantIM = meanBaseRateIM × starts
//! final = max(emissionQuantIM × IMAdjustFract
//! + emissionQuant × (1 − IMAdjustFract), 0)
//! ```
//!
//! # Algorithm
//!
//! [`CriteriaStartCalculator::calculate`] ports the SQL's "Processing"
//! section. The SQL builds ten MyISAM working tables across the numbered
//! `CSEC` steps; the port keeps each step as a function and threads the
//! working tables through as plain row vectors:
//!
//! | SQL step | SQL working table | This port |
//! |----------|-------------------|-----------|
//! | CSEC 1-a | `IMCoverageMergedUngrouped` | `im_coverage_merged` |
//! | CSEC 2-a/b | `FuelSupplyAdjustment` | `fuel_supply_adjustment` |
//! | CSEC-3 | `METStartAdjustment` | `met_start_adjustment` |
//! | CSEC-4 | `EmissionRatesWithIMAndTemp` | `emission_rates_with_im_and_temp` |
//! | CSEC-5 | `METSourceBinEmissionRates` | `met_source_bin_emission_rates` |
//! | CSEC-6 | `ActivityWeightedEmissionRate` | `activity_weighted_emission_rate` |
//! | CSEC-7 | `ActivityWeightedEmissionRate2` | `activity_weighted_emission_rate_2` |
//! | CSEC-8 | `Starts2` | `build_starts2` |
//! | CSEC-8 | `MOVESWorkerOutput` | `assemble_emission_output` |
//!
//! Every join in the SQL is an `INNER JOIN` except the CSEC 2-a
//! `LEFT OUTER JOIN criteriaRatio` — a row with no match keeps a fuel
//! adjustment of `1.0`. The port reproduces inner joins with map lookups that
//! skip on a miss, and the left join with a lookup that falls back to the
//! no-match value. CSEC 2-a cartesian-joins `County`, `PollutantProcessAssoc`,
//! `FuelFormulation` and `SourceTypeModelYear`; the port streams this product
//! without materialising it (see `fuel_supply_adjustment`), and CSEC-3
//! cartesian-joins `ZoneMonthHour` as nested loops.
//!
//! # Start temperature equation
//!
//! CSEC-3 builds the additive start-temperature adjustment per
//! `startTempEquationType`, against the `75 °F` reference: with
//! `d = min(temperature, 75) − 75`,
//!
//! * `LOG` — `tempAdjustTermB × exp(tempAdjustTermA × d) + tempAdjustTermC`;
//! * `POLY` (and any other value, the SQL `ELSE`)//! `d × (tempAdjustTermA + d × (tempAdjustTermB + d × tempAdjustTermC))`.
//!
//! The string compare is case-insensitive, matching the MariaDB
//! `utf8mb4_unicode_ci` collation the SQL runs under.
//!
//! # Scope of this port
//!
//! [`calculate`](CriteriaStartCalculator::calculate) is the SQL "Processing"
//! section. The SQL's "Extract Data" section — the `cache SELECT … INTO
//! OUTFILE` statements that filter the default and execution databases by run
//! context — is data-plane wiring, not algorithm: a [`CriteriaStartInputs`]
//! *is* the post-extract tables, so the port does not re-apply the extract
//! `WHERE` clauses (`fuelRegionID`, `yearID`, `monthID`, `zoneID`, `countyID`,
//! `polProcessID`, model-year-range filters). One processing-section filter
//! is folded into [`RunContext`]: CSEC 2-a's `WHERE ppa.polProcessID IN
//! (##pollutantProcessIDs##)` narrows `PollutantProcessAssoc` — which the
//! extract leaves at every Start Exhaust pair — to the run's pollutant set,
//! carried as [`RunContext::pol_process_ids`].
//!
//! # Fidelity notes
//!
//! `CriteriaStartCalculator.sql` stores every working-table measure in a
//! `FLOAT` (32-bit) column while MariaDB evaluates the arithmetic in
//! `DOUBLE`. This port sums, multiplies and exponentiates in `f64` end to
//! end, so it does not reproduce the `f32` truncation MOVES applies between
//! steps — a sub-`1e-7` relative drift. Reproducing it bug-for-bug is the
//! calculator-integration-validation call (Task , which this task
//! blocks), matching the / / precedent. The `FLOAT`
//! input columns (`meanBaseRate`, `meanBaseRateIM`, `temperature`,
//! `marketShare`, `sourceBinActivityFraction`, `opModeFraction`, `starts`,
//! `imFactor`, `complianceFactor`, `ratio`, `ratioGPA`, `GPAFract`,
//! `tempAdjustTermA`/`B`/`C`) are model *inputs* — already `f32`-quantised
//! before [`calculate`](CriteriaStartCalculator::calculate) sees them — and
//! are modelled as `f64`. There are no integer/integer literal divisions in
//! the SQL, so the MariaDB `div_precision_increment` rounding gotcha does not
//! arise.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm
//! is fully ported and unit-tested on
//! [`calculate`](CriteriaStartCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises a [`CriteriaStartInputs`] and a
//! [`RunContext`] from `ctx.tables()` / `ctx.position()`, gates on
//! [`processes_context`](CriteriaStartCalculator::processes_context), calls
//! [`calculate`](CriteriaStartCalculator::calculate), and writes the rows to
//! the worker output.

use rustc_hash::FxHashMap;
use std::collections::HashSet;

use rayon::join;
use rayon::slice::ParallelSliceMut;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the
/// `CriteriaStartCalculator` entry in the calculator-chain DAG
/// (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CriteriaStartCalculator";

/// Start Exhaust — `EmissionProcess` row 2. The calculator's only process: it
/// subscribes to it and the SQL produces Start Exhaust inventory.
const START_EXHAUST_PROCESS_ID: u16 = 2;

/// Off-network road type — `RoadType` row 1, `"Off-Network"`. Engine starts
/// are modelled as off-network activity; `doesProcessContext` (see
/// [`CriteriaStartCalculator::processes_context`]) runs the calculator only
/// there, and the CSEC-8 output stamps `roadTypeID = 1`.
const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

/// Reference temperature (°F) of the MOVES start-temperature equations. The
/// SQL `LEAST(temperature, 75)` caps the input here, so a start at or above
/// `75 °F` gets a zero (`POLY`) or `termB + termC` (`LOG`) adjustment.
const START_TEMP_REFERENCE_F: f64 = 75.0;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `CriteriaStartCalculator
// .sql`'s "Extract Data" section pulls. Following the convention
// every `INT`/`SMALLINT` identifier is an `i32`, `sourceBinID` (`BIGINT`) is
// an `i64`, and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only the columns
// the "Processing" section reads are modelled — the SQL also extracts
// `FuelType` and `PollutantProcessModelYear`, but neither feeds "Processing".
// ===========================================================================

/// One `AgeCategory` row — the age-group bucket for a vehicle age.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
 /// `ageID` — vehicle age in years; the unique primary key.
    pub age_id: i32,
 /// `ageGroupID` — the age-group bucket the age falls in.
    pub age_group_id: i32,
}

/// One `County` row — only `GPAFract` feeds the algorithm (CSEC 2-a blends
/// the geographic-phase-in and non-GPA fuel ratios by it).
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
/// [`model_year_id`](Self::model_year_id) is the remapped value the CSEC 2-a
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
/// `(fuelRegion, fuelYear, monthGroup)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
 /// `fuelRegionID` — the fuel region.
    pub fuel_region_id: i32,
 /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
 /// `monthGroupID` — the month group.
    pub month_group_id: i32,
 /// `fuelFormulationID` — joins to [`FuelFormulationRow::fuel_formulation_id`].
    pub fuel_formulation_id: i32,
 /// `marketShare` — the formulation's share of the fuel supply.
    pub market_share: f64,
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

/// One `MonthOfAnyYear` row — resolves a month into its month group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthOfAnyYearRow {
 /// `monthID` — calendar month.
    pub month_id: i32,
 /// `monthGroupID` — the month group the month belongs to.
    pub month_group_id: i32,
}

/// One `OpModeDistribution` row — the share of activity in one operating
/// mode. The extract filters `OpModeDistribution` to the run's link, so
/// `linkID` is not modelled.
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
/// with its model-year and I/M model-year groups.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessMappedModelYearRow {
 /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
 /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
 /// `modelYearGroupID` — the model-year group the year belongs to.
    pub model_year_group_id: i32,
 /// `IMModelYearGroupID` — the I/M model-year group the year belongs to.
    pub im_model_year_group_id: i32,
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

/// One `StartTempAdjustment` row — the start-temperature equation
/// coefficients for a `(polProcessID, modelYearGroupID, fuelTypeID,
/// opModeID)` cell. The `*CV` coefficient-of-variation columns are not read.
#[derive(Debug, Clone, PartialEq)]
pub struct StartTempAdjustmentRow {
 /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
 /// `modelYearGroupID` — the model-year group the row applies to.
    pub model_year_group_id: i32,
 /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
 /// `opModeID` — operating mode.
    pub op_mode_id: i32,
 /// `startTempEquationType` — `"LOG"`, `"POLY"`, or other (treated as
 /// `"POLY"`). Compared case-insensitively.
    pub equation_type: String,
 /// `tempAdjustTermA` — equation coefficient A.
    pub term_a: f64,
 /// `tempAdjustTermB` — equation coefficient B.
    pub term_b: f64,
 /// `tempAdjustTermC` — equation coefficient C.
    pub term_c: f64,
}

/// One `Starts` row — engine-start activity for a `(hourDay, month, year,
/// age, zone, sourceType)` cell. Engine starts are zone-level activity.
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

/// One `Year` row — resolves a calendar year into its fuel year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
 /// `yearID` — calendar year.
    pub year_id: i32,
 /// `fuelYearID` — the fuel year the calendar year maps to.
    pub fuel_year_id: i32,
}

/// One `Zone` row — supplies the `countyID` for a zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRow {
 /// `zoneID` — the zone primary key.
    pub zone_id: i32,
 /// `countyID` — the county the zone belongs to.
    pub county_id: i32,
}

/// One `ZoneMonthHour` row — the temperature for a `(zone, month, hour)`
/// cell. Only `temperature` feeds the algorithm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
 /// `zoneID` — the zone.
    pub zone_id: i32,
 /// `monthID` — calendar month.
    pub month_id: i32,
 /// `hourID` — hour of day.
    pub hour_id: i32,
 /// `temperature` — ambient temperature, °F. `FLOAT` in MOVES.
    pub temperature: f64,
}

/// Inputs to [`CriteriaStartCalculator::calculate`] — the tables the SQL's
/// "Extract Data" section produces, as plain row vectors.
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct CriteriaStartInputs {
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
 /// `PollutantProcessMappedModelYear` rows.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
 /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
 /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
 /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
 /// `StartTempAdjustment` rows.
    pub start_temp_adjustment: Vec<StartTempAdjustmentRow>,
 /// `Starts` rows.
    pub starts: Vec<StartsRow>,
 /// `Year` rows.
    pub year: Vec<YearRow>,
 /// `Zone` rows.
    pub zone: Vec<ZoneRow>,
 /// `ZoneMonthHour` rows.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
}

/// The per-run scalar context [`CriteriaStartCalculator::calculate`] reads/// the `##context.*##` substitutions the SQL preprocessor resolves before
/// running the script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunContext {
 /// `##context.year##` — the run's calendar year. Used to derive vehicle
 /// model year (`year - ageID`) in CSEC 1-a, CSEC-4 and CSEC-8, and
 /// stamped as `yearID` on the CSEC-4 emission rates.
    pub year: i32,
 /// `##context.fuelRegionID##` — the run's fuel region. Stamped on
 /// `CountyFuelAdjustment` (CSEC 2-a) and joined against
 /// `FuelSupply.fuelRegionID`.
    pub fuel_region_id: i32,
 /// `##context.iterLocation.countyRecordID##` — the run's county. Stamped
 /// as `countyID` on `FuelSupplyAdjustment` and on the worker output.
    pub county_id: i32,
 /// `##context.iterLocation.linkRecordID##` — the run's link. Stamped as
 /// `linkID` on the worker output.
    pub link_id: i32,
 /// `##context.iterLocation.stateRecordID##` — the run's state. Stamped as
 /// `stateID` on the worker output.
    pub state_id: i32,
 /// `##pollutantProcessIDs##` — the run's `polProcessID`s, the
 /// RunSpec-intersected subset of the calculator's `{102, 202, 302}`
 /// capability. CSEC 2-a filters `PollutantProcessAssoc` to this set (the
 /// only processing-section `WHERE` clause not folded into the extract).
    pub pol_process_ids: Vec<i32>,
}

/// One `MOVESWorkerOutput` row produced by the criteria start calculation/// the CSEC-8 output, with the I/M blend applied.
///
/// The SQL writes an `SCC` column `NULL`; it is not an algorithm input and is
/// left to the output wiring. `emission_quant` carries the final,
/// I/M-adjusted emission; the SQL's intermediate `emissionQuantIM` column is
/// dropped before the worker output is returned, so it is not modelled here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CriteriaStartEmissionRow {
 /// `stateID`.
    pub state_id: i32,
 /// `countyID`.
    pub county_id: i32,
 /// `zoneID`.
    pub zone_id: i32,
 /// `linkID`.
    pub link_id: i32,
 /// `roadTypeID` — always `1` (off-network); engine starts are off-network.
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
 /// `processID` — always `2` (Start Exhaust).
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

impl CriteriaStartEmissionRow {
 /// The integer dimension tuple — every column except `emission_quant`.
 /// Used to sort the output deterministically: MOVES leaves
 /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT`
 /// has no `ORDER BY`), so the port sorts purely to make the result
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
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

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

impl TableRow for CountyRow {
    fn table_name() -> &'static str {
        "County"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("GPAFract".into(), DataType::Float64),
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
                    "GPAFract".into(),
                    rows.iter().map(|r| r.gpa_fract).collect::<Vec<f64>>(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let county_id = get_i32("countyID")?;
        let gpa_fract = get_f64("GPAFract")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CountyRow {
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    gpa_fract: gpa_fract.get(i).ok_or_else(|| null("GPAFract"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CriteriaRatioRow {
    fn table_name() -> &'static str {
        "criteriaRatio"
    }
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
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
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
                Series::new(
                    "ratio".into(),
                    rows.iter().map(|r| r.ratio).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ratioGPA".into(),
                    rows.iter().map(|r| r.ratio_gpa).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "criteriaRatio";
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
        let pol_process_id = get_i32("polProcessID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let ratio = get_f64("ratio")?;
        let ratio_gpa = get_f64("ratioGPA")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CriteriaRatioRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    ratio: ratio.get(i).ok_or_else(|| null("ratio"))?,
                    ratio_gpa: ratio_gpa.get(i).ok_or_else(|| null("ratioGPA"))?,
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
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let mean_base_rate = get_f64("meanBaseRate")?;
        let mean_base_rate_im = get_f64("meanBaseRateIM")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRateByAgeRow {
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mean_base_rate_im
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIM"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let fuel_subtype_id = get_i32("fuelSubtypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_subtype_id: fuel_subtype_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubtypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSubtypeRow {
    fn table_name() -> &'static str {
        "FuelSubtype"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
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
        let t = "FuelSubtype";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_subtype_id = get_i32("fuelSubtypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubtypeRow {
                    fuel_subtype_id: fuel_subtype_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelRegionID".into(), DataType::Int32),
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
                Series::new(
                    "fuelRegionID".into(),
                    rows.iter().map(|r| r.fuel_region_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "marketShare".into(),
                    rows.iter().map(|r| r.market_share).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
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
        let fuel_region_id = get_i32("fuelRegionID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        let month_group_id = get_i32("monthGroupID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    fuel_region_id: fuel_region_id.get(i).ok_or_else(|| null("fuelRegionID"))?,
                    fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                    month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
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
        let hour_day_id = get_i32("hourDayID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let beg_model_year_id = get_i32("begModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let compliance_factor = get_f64("complianceFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ImCoverageRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    beg_model_year_id: beg_model_year_id
                        .get(i)
                        .ok_or_else(|| null("begModelYearID"))?,
                    end_model_year_id: end_model_year_id
                        .get(i)
                        .ok_or_else(|| null("endModelYearID"))?,
                    inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                    test_standards_id: test_standards_id
                        .get(i)
                        .ok_or_else(|| null("testStandardsID"))?,
                    compliance_factor: compliance_factor
                        .get(i)
                        .ok_or_else(|| null("complianceFactor"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let im_factor = get_f64("IMFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ImFactorRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                    test_standards_id: test_standards_id
                        .get(i)
                        .ok_or_else(|| null("testStandardsID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    im_model_year_group_id: im_model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    im_factor: im_factor.get(i).ok_or_else(|| null("IMFactor"))?,
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
        let month_id = get_i32("monthID")?;
        let month_group_id = get_i32("monthGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MonthOfAnyYearRow {
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
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
        let pol_process_id = get_i32("polProcessID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PollutantProcessMappedModelYearRow {
    fn table_name() -> &'static str {
        "PollutantProcessMappedModelYear"
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
        let t = "PollutantProcessMappedModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_id = get_i32("modelYearID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessMappedModelYearRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                    im_model_year_group_id: im_model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
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
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
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
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let source_bin_id = get_i64("sourceBinID")?;
        let source_bin_activity_fraction = get_f64("sourceBinActivityFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinDistributionRow {
                    source_type_model_year_id: source_type_model_year_id
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    source_bin_activity_fraction: source_bin_activity_fraction
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
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeModelYearRow {
                    source_type_model_year_id: source_type_model_year_id
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartTempAdjustmentRow {
    fn table_name() -> &'static str {
        "StartTempAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("startTempEquationType".into(), DataType::String),
            ("tempAdjustTermA".into(), DataType::Float64),
            ("tempAdjustTermB".into(), DataType::Float64),
            ("tempAdjustTermC".into(), DataType::Float64),
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
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "startTempEquationType".into(),
                    rows.iter()
                        .map(|r| r.equation_type.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermA".into(),
                    rows.iter().map(|r| r.term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermB".into(),
                    rows.iter().map(|r| r.term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermC".into(),
                    rows.iter().map(|r| r.term_c).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "StartTempAdjustment";
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
        let get_str = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .str()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let op_mode_id = get_i32("opModeID")?;
        let equation_type = get_str("startTempEquationType")?;
        let term_a = get_f64("tempAdjustTermA")?;
        let term_b = get_f64("tempAdjustTermB")?;
        let term_c = get_f64("tempAdjustTermC")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartTempAdjustmentRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    equation_type: equation_type
                        .get(i)
                        .ok_or_else(|| null("startTempEquationType"))?
                        .to_string(),
                    term_a: term_a.get(i).ok_or_else(|| null("tempAdjustTermA"))?,
                    term_b: term_b.get(i).ok_or_else(|| null("tempAdjustTermB"))?,
                    term_c: term_c.get(i).ok_or_else(|| null("tempAdjustTermC"))?,
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let starts = get_f64("starts")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartsRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
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
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Year";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(YearRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
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
        let zone_id = get_i32("zoneID")?;
        let county_id = get_i32("countyID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
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
            ("zoneID".into(), DataType::Int32),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneMonthHourRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    temperature: temperature.get(i).ok_or_else(|| null("temperature"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CriteriaStartEmissionRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }
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
        DataFrame::new(
            n,
            vec![
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
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
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
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CriteriaStartEmissionRow {
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
            })
            .collect()
    }
}

// ===========================================================================
// Working tables — the intermediate MyISAM tables the SQL's "Processing"
// section builds. All are private to the module; each is the output of one
// CSEC step and the input of the next.
// ===========================================================================

/// CSEC 1-a — `IMCoverageMergedUngrouped`: the summed inspection-and-
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

/// CSEC 2-b — `FuelSupplyAdjustment`: the market-share-weighted fuel
/// adjustment per `(year, month, polProcess, modelYear, sourceType,
/// fuelType)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelSupplyAdjustment {
    year_id: i32,
    county_id: i32,
    month_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    fuel_adjustment: f64,
}

/// CSEC-3 — `METStartAdjustment`: the additive start-temperature adjustment
/// per `(zone, month, hour, polProcess, modelYear, fuelType, opMode)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MetStartAdjustment {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    op_mode_id: i32,
    temperature_adjustment: f64,
}

/// CSEC-4 — `EmissionRatesWithIMAndTemp`: the per-source-bin emission rate
/// with the start-temperature adjustment added.
#[derive(Debug, Clone, Copy, PartialEq)]
struct EmissionRatesWithImAndTemp {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    year_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    source_bin_id: i64,
    op_mode_id: i32,
    fuel_type_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CSEC-5 — `METSourceBinEmissionRates`: [`EmissionRatesWithImAndTemp`]
/// weighted by source-bin activity fraction and aggregated to source type.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MetSourceBinEmissionRates {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    year_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    op_mode_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CSEC-6 — `ActivityWeightedEmissionRate`: [`MetSourceBinEmissionRates`]
/// weighted by operating-mode fraction and aggregated over operating mode.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActivityWeightedEmissionRate {
    zone_id: i32,
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CSEC-7 — `ActivityWeightedEmissionRate2`: [`ActivityWeightedEmissionRate`]
/// multiplied by the fuel-supply adjustment.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActivityWeightedEmissionRate2 {
    zone_id: i32,
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// CSEC-8 — `Starts2`: the `Starts` table re-keyed by model year, with the
/// hour-day surrogate resolved to `(dayID, hourID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Starts2 {
    zone_id: i32,
    month_id: i32,
    hour_id: i32,
    day_id: i32,
    year_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    starts: f64,
}

// ===========================================================================
// Processing steps — one function per CSEC step. Each takes the inputs (and
// any prior working tables) and returns the next working table as a `Vec`.
// ===========================================================================

/// CSEC 1-a — `IMCoverageMergedUngrouped`.
///
/// `IMAdjustFract = Σ(imFactor × complianceFactor × 0.01)`, summed over the
/// `PollutantProcessMappedModelYear ⋈ PollutantProcessAssoc ⋈ IMFactor ⋈
/// AgeCategory ⋈ IMCoverage` join and grouped by `(processID, pollutantID,
/// modelYearID, fuelTypeID, sourceTypeID)`.
///
/// The `AgeCategory` join plus the `ppmy.modelYearID = year - ageID` filter
/// together require `IMFactor.ageGroupID` to be the age group of the age
/// `year - ppmy.modelYearID`; the port resolves that age once and compares.
fn im_coverage_merged(inputs: &CriteriaStartInputs, ctx: &RunContext) -> Vec<ImCoverageMerged> {
 // PollutantProcessAssoc lookup — polProcessID → (pollutantID, processID).
    let ppa: FxHashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
 // AgeCategory lookup — ageID → ageGroupID. `ageID` is the unique key.
    let age_group_by_age: FxHashMap<i32, i32> = inputs
        .age_category
        .iter()
        .map(|r| (r.age_id, r.age_group_id))
        .collect();
 // IMFactor indexed for the `(polProcessID, IMModelYearGroupID)` join.
    let mut imf_by_key: FxHashMap<(i32, i32), Vec<&ImFactorRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.im_factor.len(), Default::default());
    for imf in &inputs.im_factor {
        imf_by_key
            .entry((imf.pol_process_id, imf.im_model_year_group_id))
            .or_default()
            .push(imf);
    }
 // IMCoverage indexed for the five-column equality join; the model-year
 // range is filtered per matched row.
    let mut imc_by_key: FxHashMap<(i32, i32, i32, i32, i32), Vec<&ImCoverageRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.im_coverage.len(), Default::default());
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
    let mut totals: FxHashMap<[i32; 5], f64> = FxHashMap::with_capacity_and_hasher(
        inputs.pollutant_process_mapped_model_year.len(),
        Default::default(),
    );
    for ppmy in &inputs.pollutant_process_mapped_model_year {
 // INNER JOIN PollutantProcessAssoc USING (polProcessID).
        let Some(assoc) = ppa.get(&ppmy.pol_process_id) else {
            continue;
        };
 // INNER JOIN IMFactor ON (polProcessID, IMModelYearGroupID).
        let Some(imfs) = imf_by_key.get(&(ppmy.pol_process_id, ppmy.im_model_year_group_id)) else {
            continue;
        };
 // The single age whose model year is `ppmy.modelYearID` // `ppmy.modelYearID = year - ageID`.
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

/// CSEC 2-a/b — `FuelSupplyAdjustment`.
///
/// Fuses CSEC 2-a (`CountyFuelAdjustment`) and CSEC 2-b
/// (`CountyFuelAdjustmentWithFuelType`, `FuelSupplyAdjustment`) into a single
/// streaming pass that never materialises the cartesian product.
///
/// The SQL CSEC 2-a cartesian-joins `County × PollutantProcessAssoc ×
/// FuelFormulation × SourceTypeModelYear` (786 × 2158 × 533 ≈ 904 M rows on
/// a real runspec) with a left join to `criteriaRatio`. CSEC 2-b immediately
/// inner-joins each row to `FuelFormulation ⋈ FuelSubtype` to resolve the
/// fuel type, then aggregates `Σ(fuelAdjustment × marketShare)` against
/// `FuelSupply` grouped by `(year, month, polProcess, modelYear, sourceType,
/// fuelType)`.
///
/// This implementation streams the cartesian product as nested loops without
/// pushing any intermediate row into a `Vec`. For each `FuelFormulation` the
/// fuel-type lookup and the matching `FuelSupply` rows are resolved once
/// (before the inner `SourceTypeModelYear` loop), so formulations that have
/// no fuel-type mapping or no supply entry are skipped cheaply. The only
/// collection that grows is the `totals` accumulator, which is bounded by the
/// number of distinct aggregation keys — orders of magnitude smaller than the
/// cartesian product.
fn fuel_supply_adjustment(
    inputs: &CriteriaStartInputs,
    ctx: &RunContext,
) -> Vec<FuelSupplyAdjustment> {
    let pol_process_ids: HashSet<i32> = ctx.pol_process_ids.iter().copied().collect();

 // criteriaRatio indexed for the CSEC 2-a LEFT JOIN — one key may carry
 // several rows; the join emits one output row per match.
    let mut cr_by_key: FxHashMap<(i32, i32, i32, i32), Vec<&CriteriaRatioRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.criteria_ratio.len(), Default::default());
    for cr in &inputs.criteria_ratio {
        cr_by_key
            .entry((
                cr.pol_process_id,
                cr.fuel_formulation_id,
                cr.source_type_id,
                cr.model_year_id,
            ))
            .or_default()
            .push(cr);
    }

 // fuelFormulationID → fuelSubtypeID (CSEC 2-b INNER JOIN FuelFormulation).
    let subtype_of_formulation: FxHashMap<i32, i32> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff.fuel_subtype_id))
        .collect();
 // fuelSubtypeID → fuelTypeID (CSEC 2-b INNER JOIN FuelSubtype).
    let fuel_type_of_subtype: FxHashMap<i32, i32> = inputs
        .fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst.fuel_type_id))
        .collect();

 // FuelSupply keyed by (fuelRegionID, fuelYearID, monthGroupID,
 // fuelFormulationID) for the CSEC 2-b INNER JOIN.
    let fs_by_key: FxHashMap<(i32, i32, i32, i32), &FuelSupplyRow> = inputs
        .fuel_supply
        .iter()
        .map(|fs| {
            (
                (
                    fs.fuel_region_id,
                    fs.fuel_year_id,
                    fs.month_group_id,
                    fs.fuel_formulation_id,
                ),
                fs,
            )
        })
        .collect();

 // CSEC 2-a joins `County`, but the original SQL is parameterised by the
 // context county (`##context.iterLocation.countyRecordID##`): only the
 // current county's `GPAFract` participates. The replayed execution-DB
 // snapshot carries *every* county's `County` row (3232 at national scale),
 // so resolve the context county's `GPAFract` once here. Looping the whole
 // `County` table would (a) multiply the work by the county count — the
 // source of the observed national-scale hang — and (b) fold every county's
 // GPA contribution into the county-agnostic `totals` keys, inflating the
 // result. A missing county row leaves `GPAFract = 0` (the GPA term drops
 // out), matching the LEFT-JOIN-style default.
    let gpa_fract = inputs
        .county
        .iter()
        .find(|c| c.county_id == ctx.county_id)
        .map_or(0.0, |c| c.gpa_fract);

 // GROUP BY (yearID, monthID, polProcessID, modelYearID, sourceTypeID,
 // fuelTypeID). The capacity is an upper bound on the distinct key count;
 // cap it so a wide national runspec (many years × months × pol-processes)
 // cannot request a pathologically large single allocation.
    let totals_cap = (inputs.year.len())
        .saturating_mul(inputs.month_of_any_year.len())
        .saturating_mul(pol_process_ids.len())
        .saturating_mul(inputs.source_type_model_year.len())
        .min(1 << 22);
    let mut totals: FxHashMap<[i32; 6], f64> =
        FxHashMap::with_capacity_and_hasher(totals_cap, Default::default());

 // Iterate FuelFormulation outermost so the per-formulation supply lookup
 // runs once each. `supply_weights` and the resolved fuel type depend only
 // on the formulation (not the pollutant-process), so hoisting the
 // PollutantProcessAssoc loop inside lets formulations with no supply skip
 // the entire (ppa × SourceTypeModelYear) inner product. The original SQL's
 // cartesian join `County × PollutantProcessAssoc × FuelFormulation ×
 // SourceTypeModelYear` is summed into `totals`, so reordering the loops is
 // result-preserving (addition is commutative).
    let mut supply_weights: Vec<(i32, i32, f64)> =
        Vec::with_capacity(inputs.year.len() * inputs.month_of_any_year.len());
    for ff in &inputs.fuel_formulation {
 // Resolve fuel type once per formulation; skip everything downstream
 // if either lookup fails.
        let Some(&subtype_id) = subtype_of_formulation.get(&ff.fuel_formulation_id) else {
            continue;
        };
        let Some(&fuel_type_id) = fuel_type_of_subtype.get(&subtype_id) else {
            continue;
        };

 // Collect (year_id, month_id, market_share) triples for this
 // formulation once — reused for every (ppa, SourceTypeModelYear) pair.
        supply_weights.clear();
        for year in &inputs.year {
            for may in &inputs.month_of_any_year {
                if let Some(fs) = fs_by_key.get(&(
                    ctx.fuel_region_id,
                    year.fuel_year_id,
                    may.month_group_id,
                    ff.fuel_formulation_id,
                )) {
                    supply_weights.push((year.year_id, may.month_id, fs.market_share));
                }
            }
        }

        if supply_weights.is_empty() {
            continue;
        }

        for ppa in &inputs.pollutant_process_assoc {
 // CSEC 2-a `WHERE ppa.polProcessID IN (##pollutantProcessIDs##)`.
            if !pol_process_ids.contains(&ppa.pol_process_id) {
                continue;
            }
            for stmy in &inputs.source_type_model_year {
                let matches = cr_by_key.get(&(
                    ppa.pol_process_id,
                    ff.fuel_formulation_id,
                    stmy.source_type_id,
                    stmy.model_year_id,
                ));

                let mut accumulate = |fuel_adjustment: f64| {
                    for &(year_id, month_id, market_share) in &supply_weights {
 *totals
                            .entry([
                                year_id,
                                month_id,
                                ppa.pol_process_id,
                                stmy.model_year_id,
                                stmy.source_type_id,
                                fuel_type_id,
                            ])
                            .or_default() += fuel_adjustment * market_share;
                    }
                };

                match matches {
 // LEFT JOIN miss — ifnull(NULL, 1) gives 1.0.
                    None => accumulate(1.0),
                    Some(crs) => {
                        for cr in crs {
                            accumulate(cr.ratio + gpa_fract * (cr.ratio_gpa - cr.ratio));
                        }
                    }
                }
            }
        }
    }

    totals
        .into_iter()
        .map(|(k, fuel_adjustment)| FuelSupplyAdjustment {
            year_id: k[0],
            county_id: ctx.county_id,
            month_id: k[1],
            pol_process_id: k[2],
            model_year_id: k[3],
            source_type_id: k[4],
            fuel_type_id: k[5],
            fuel_adjustment,
        })
        .collect()
}

/// CSEC-3 — `METStartAdjustment`.
///
/// The cartesian product `StartTempAdjustment ⋈ PollutantProcessMappedModel
/// Year × ZoneMonthHour` with the start-temperature equation applied per
/// `startTempEquationType`. `ZoneMonthHour` is extract-filtered to the run's
/// zone.
fn met_start_adjustment(inputs: &CriteriaStartInputs) -> Vec<MetStartAdjustment> {
 // PollutantProcessMappedModelYear indexed for the `(polProcessID,
 // modelYearGroupID)` join — a group spans several model years.
    let mut ppmy_by_key: FxHashMap<(i32, i32), Vec<&PollutantProcessMappedModelYearRow>> =
        FxHashMap::with_capacity_and_hasher(
            inputs.pollutant_process_mapped_model_year.len(),
            Default::default(),
        );
    for ppmy in &inputs.pollutant_process_mapped_model_year {
        ppmy_by_key
            .entry((ppmy.pol_process_id, ppmy.model_year_group_id))
            .or_default()
            .push(ppmy);
    }

    let mut out: Vec<MetStartAdjustment> = Vec::new();
    for sta in &inputs.start_temp_adjustment {
 // INNER JOIN PollutantProcessMappedModelYear ON (polProcessID,
 // modelYearGroupID).
        let Some(ppmys) = ppmy_by_key.get(&(sta.pol_process_id, sta.model_year_group_id)) else {
            continue;
        };
        for ppmy in ppmys {
            for zmh in &inputs.zone_month_hour {
                let delta = zmh.temperature.min(START_TEMP_REFERENCE_F) - START_TEMP_REFERENCE_F;
                let temperature_adjustment = if sta.equation_type.eq_ignore_ascii_case("LOG") {
                    sta.term_b * (sta.term_a * delta).exp() + sta.term_c
                } else {
 // "POLY" and the SQL `ELSE` branch — Horner form of
 // `d·(A + d·(B + d·C))`.
                    delta * (sta.term_a + delta * (sta.term_b + delta * sta.term_c))
                };
                out.push(MetStartAdjustment {
                    zone_id: zmh.zone_id,
                    month_id: zmh.month_id,
                    hour_id: zmh.hour_id,
                    pol_process_id: sta.pol_process_id,
                    model_year_id: ppmy.model_year_id,
                    fuel_type_id: sta.fuel_type_id,
                    op_mode_id: sta.op_mode_id,
                    temperature_adjustment,
                });
            }
        }
    }
    out
}

/// CSEC-4 — `EmissionRatesWithIMAndTemp`.
///
/// `SourceBin ⋈ EmissionRateByAge ⋈ AgeCategory ⋈ METStartAdjustment`, with
/// the start-temperature adjustment added to both the non-I/M and I/M base
/// rates. The `AgeCategory` join expands each emission rate across its
/// age group; the `METStartAdjustment` join then keys on `modelYearID =
/// year - ageID`.
fn emission_rates_with_im_and_temp(
    inputs: &CriteriaStartInputs,
    ctx: &RunContext,
    met_start: &[MetStartAdjustment],
) -> Vec<EmissionRatesWithImAndTemp> {
 // SourceBin lookup — sourceBinID → fuelTypeID.
    let source_bin: FxHashMap<i64, &SourceBinRow> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb))
        .collect();
 // AgeCategory indexed by age group — each group holds several ages.
    let mut ages_by_group: FxHashMap<i32, Vec<&AgeCategoryRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.age_category.len(), Default::default());
    for age in &inputs.age_category {
        ages_by_group.entry(age.age_group_id).or_default().push(age);
    }
 // METStartAdjustment indexed for the four-column join.
    let mut msa_by_key: FxHashMap<(i32, i32, i32, i32), Vec<&MetStartAdjustment>> =
        FxHashMap::with_capacity_and_hasher(met_start.len(), Default::default());
    for msa in met_start {
        msa_by_key
            .entry((
                msa.pol_process_id,
                msa.model_year_id,
                msa.op_mode_id,
                msa.fuel_type_id,
            ))
            .or_default()
            .push(msa);
    }

    let mut out: Vec<EmissionRatesWithImAndTemp> = Vec::new();
    for erim in &inputs.emission_rate_by_age {
 // INNER JOIN SourceBin USING (sourceBinID).
        let Some(sb) = source_bin.get(&erim.source_bin_id) else {
            continue;
        };
 // INNER JOIN AgeCategory USING (ageGroupID).
        let Some(ages) = ages_by_group.get(&erim.age_group_id) else {
            continue;
        };
        for age in ages {
 // INNER JOIN METStartAdjustment ON (polProcessID,
 // modelYearID = year - ageID, opModeID, fuelTypeID).
            let model_year_id = ctx.year - age.age_id;
            let Some(msas) = msa_by_key.get(&(
                erim.pol_process_id,
                model_year_id,
                erim.op_mode_id,
                sb.fuel_type_id,
            )) else {
                continue;
            };
            for msa in msas {
                out.push(EmissionRatesWithImAndTemp {
                    zone_id: msa.zone_id,
                    month_id: msa.month_id,
                    hour_id: msa.hour_id,
                    year_id: ctx.year,
                    pol_process_id: msa.pol_process_id,
                    model_year_id: msa.model_year_id,
                    source_bin_id: erim.source_bin_id,
                    op_mode_id: msa.op_mode_id,
                    fuel_type_id: msa.fuel_type_id,
                    mean_base_rate: erim.mean_base_rate + msa.temperature_adjustment,
                    mean_base_rate_im: erim.mean_base_rate_im + msa.temperature_adjustment,
                });
            }
        }
    }
    out
}

/// CSEC-5 — `METSourceBinEmissionRates`.
///
/// `meanBaseRate = Σ(meanBaseRate × sourceBinActivityFraction)`, summed over
/// `EmissionRatesWithIMAndTemp ⋈ SourceBinDistribution ⋈ SourceTypeModelYear`
/// and grouped by `(zone, month, hour, year, polProcess, sourceType,
/// modelYear, fuelType, opMode)`.
fn met_source_bin_emission_rates(
    inputs: &CriteriaStartInputs,
    emission_rates: &[EmissionRatesWithImAndTemp],
) -> Vec<MetSourceBinEmissionRates> {
 // SourceBinDistribution indexed for the `(polProcessID, sourceBinID)`
 // join.
    let mut sbd_by_key: FxHashMap<(i32, i64), Vec<&SourceBinDistributionRow>> =
        FxHashMap::with_capacity_and_hasher(
            inputs.source_bin_distribution.len(),
            Default::default(),
        );
    for sbd in &inputs.source_bin_distribution {
        sbd_by_key
            .entry((sbd.pol_process_id, sbd.source_bin_id))
            .or_default()
            .push(sbd);
    }
 // SourceTypeModelYear keyed by its `sourceTypeModelYearID` primary key.
    let stmy_by_id: FxHashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();

 // GROUP BY (zone, month, hour, year, polProcess, sourceType, modelYear,
 // fuelType, opMode).
    let mut totals: FxHashMap<[i32; 9], (f64, f64)> =
        FxHashMap::with_capacity_and_hasher(emission_rates.len(), Default::default());
    for er in emission_rates {
 // INNER JOIN SourceBinDistribution ON (polProcessID, sourceBinID).
        let Some(sbds) = sbd_by_key.get(&(er.pol_process_id, er.source_bin_id)) else {
            continue;
        };
        for sbd in sbds {
 // INNER JOIN SourceTypeModelYear USING (sourceTypeModelYearID)
 // AND er.modelYearID = stmy.modelYearID.
            let Some(stmy) = stmy_by_id.get(&sbd.source_type_model_year_id) else {
                continue;
            };
            if er.model_year_id != stmy.model_year_id {
                continue;
            }
            let entry = totals
                .entry([
                    er.zone_id,
                    er.month_id,
                    er.hour_id,
                    er.year_id,
                    er.pol_process_id,
                    stmy.source_type_id,
                    stmy.model_year_id,
                    er.fuel_type_id,
                    er.op_mode_id,
                ])
                .or_insert((0.0, 0.0));
            entry.0 += er.mean_base_rate * sbd.source_bin_activity_fraction;
            entry.1 += er.mean_base_rate_im * sbd.source_bin_activity_fraction;
        }
    }

    totals
        .into_iter()
        .map(
            |(k, (mean_base_rate, mean_base_rate_im))| MetSourceBinEmissionRates {
                zone_id: k[0],
                month_id: k[1],
                hour_id: k[2],
                year_id: k[3],
                pol_process_id: k[4],
                source_type_id: k[5],
                model_year_id: k[6],
                fuel_type_id: k[7],
                op_mode_id: k[8],
                mean_base_rate,
                mean_base_rate_im,
            },
        )
        .collect()
}

/// CSEC-6 — `ActivityWeightedEmissionRate`.
///
/// `meanBaseRate = Σ(meanBaseRate × opModeFraction)`, summed over
/// `METSourceBinEmissionRates ⋈ HourDay ⋈ OpModeDistribution` and grouped by
/// `(zone, year, month, day, hour, polProcess, sourceType, modelYear,
/// fuelType)` — collapsing the operating-mode dimension.
fn activity_weighted_emission_rate(
    inputs: &CriteriaStartInputs,
    met_source_bin: &[MetSourceBinEmissionRates],
) -> Vec<ActivityWeightedEmissionRate> {
 // HourDay indexed by hour — an hour spans several day types.
    let mut hour_days_by_hour: FxHashMap<i32, Vec<&HourDayRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.hour_day.len(), Default::default());
    for hd in &inputs.hour_day {
        hour_days_by_hour.entry(hd.hour_id).or_default().push(hd);
    }
 // OpModeDistribution indexed for the four-column join.
    let mut omd_by_key: FxHashMap<(i32, i32, i32, i32), Vec<&OpModeDistributionRow>> =
        FxHashMap::with_capacity_and_hasher(inputs.op_mode_distribution.len(), Default::default());
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

 // GROUP BY (zone, year, month, day, hour, polProcess, sourceType,
 // modelYear, fuelType).
    let mut totals: FxHashMap<[i32; 9], (f64, f64)> =
        FxHashMap::with_capacity_and_hasher(met_source_bin.len(), Default::default());
    for msber in met_source_bin {
 // INNER JOIN HourDay USING (hourID).
        let Some(hour_days) = hour_days_by_hour.get(&msber.hour_id) else {
            continue;
        };
        for hd in hour_days {
 // INNER JOIN OpModeDistribution ON (sourceTypeID, hourDayID,
 // polProcessID, opModeID).
            let Some(omds) = omd_by_key.get(&(
                msber.source_type_id,
                hd.hour_day_id,
                msber.pol_process_id,
                msber.op_mode_id,
            )) else {
                continue;
            };
            for omd in omds {
                let entry = totals
                    .entry([
                        msber.zone_id,
                        msber.year_id,
                        msber.month_id,
                        hd.day_id,
                        msber.hour_id,
                        msber.pol_process_id,
                        msber.source_type_id,
                        msber.model_year_id,
                        msber.fuel_type_id,
                    ])
                    .or_insert((0.0, 0.0));
                entry.0 += msber.mean_base_rate * omd.op_mode_fraction;
                entry.1 += msber.mean_base_rate_im * omd.op_mode_fraction;
            }
        }
    }

    totals
        .into_iter()
        .map(
            |(k, (mean_base_rate, mean_base_rate_im))| ActivityWeightedEmissionRate {
                zone_id: k[0],
                year_id: k[1],
                month_id: k[2],
                day_id: k[3],
                hour_id: k[4],
                pol_process_id: k[5],
                source_type_id: k[6],
                model_year_id: k[7],
                fuel_type_id: k[8],
                mean_base_rate,
                mean_base_rate_im,
            },
        )
        .collect()
}

/// CSEC-7 — `ActivityWeightedEmissionRate2`.
///
/// Multiplies each [`ActivityWeightedEmissionRate`] by its fuel-supply
/// adjustment: `ActivityWeightedEmissionRate ⋈ FuelSupplyAdjustment ⋈ Zone`,
/// where the `Zone` join confirms the rate's zone belongs to the fuel
/// adjustment's county.
fn activity_weighted_emission_rate_2(
    inputs: &CriteriaStartInputs,
    activity_weighted: &[ActivityWeightedEmissionRate],
    fuel_supply_adj: &[FuelSupplyAdjustment],
) -> Vec<ActivityWeightedEmissionRate2> {
 // FuelSupplyAdjustment indexed for the six-column join; the county is
 // checked through the Zone join.
    let mut fsa_by_key: FxHashMap<[i32; 6], Vec<&FuelSupplyAdjustment>> =
        FxHashMap::with_capacity_and_hasher(fuel_supply_adj.len(), Default::default());
    for fsa in fuel_supply_adj {
        fsa_by_key
            .entry([
                fsa.year_id,
                fsa.month_id,
                fsa.pol_process_id,
                fsa.model_year_id,
                fsa.source_type_id,
                fsa.fuel_type_id,
            ])
            .or_default()
            .push(fsa);
    }
 // Zone keyed by its `zoneID` primary key.
    let zone_by_id: FxHashMap<i32, &ZoneRow> = inputs.zone.iter().map(|z| (z.zone_id, z)).collect();

    let mut out: Vec<ActivityWeightedEmissionRate2> = Vec::new();
    for awer in activity_weighted {
 // INNER JOIN FuelSupplyAdjustment ON (yearID, monthID, polProcessID,
 // modelYearID, sourceTypeID, fuelTypeID).
        let Some(fsas) = fsa_by_key.get(&[
            awer.year_id,
            awer.month_id,
            awer.pol_process_id,
            awer.model_year_id,
            awer.source_type_id,
            awer.fuel_type_id,
        ]) else {
            continue;
        };
 // INNER JOIN Zone ON (z.zoneID = awer.zoneID).
        let Some(zone) = zone_by_id.get(&awer.zone_id) else {
            continue;
        };
        for fsa in fsas {
 // … AND z.countyID = fsa.countyID.
            if zone.county_id != fsa.county_id {
                continue;
            }
            out.push(ActivityWeightedEmissionRate2 {
                zone_id: awer.zone_id,
                year_id: awer.year_id,
                month_id: awer.month_id,
                day_id: awer.day_id,
                hour_id: awer.hour_id,
                pol_process_id: awer.pol_process_id,
                source_type_id: awer.source_type_id,
                model_year_id: awer.model_year_id,
                fuel_type_id: awer.fuel_type_id,
                mean_base_rate: awer.mean_base_rate * fsa.fuel_adjustment,
                mean_base_rate_im: awer.mean_base_rate_im * fsa.fuel_adjustment,
            });
        }
    }
    out
}

/// CSEC-8 — `Starts2`.
///
/// Re-keys the `Starts` activity by model year (`year - ageID`) and resolves
/// the `hourDayID` surrogate to `(dayID, hourID)` through `HourDay`.
fn build_starts2(inputs: &CriteriaStartInputs, ctx: &RunContext) -> Vec<Starts2> {
 // HourDay keyed by its `hourDayID` primary key.
    let hour_day_by_id: FxHashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();

    let mut out: Vec<Starts2> = Vec::new();
    for st in &inputs.starts {
 // INNER JOIN HourDay USING (hourDayID).
        let Some(hd) = hour_day_by_id.get(&st.hour_day_id) else {
            continue;
        };
        out.push(Starts2 {
            zone_id: st.zone_id,
            month_id: st.month_id,
            hour_id: hd.hour_id,
            day_id: hd.day_id,
            year_id: st.year_id,
            source_type_id: st.source_type_id,
            model_year_id: ctx.year - st.age_id,
            starts: st.starts,
        });
    }
    out
}

/// CSEC-8 — `MOVESWorkerOutput` plus the I/M `update`.
///
/// `Starts2 ⋈ ActivityWeightedEmissionRate2 ⋈ PollutantProcessAssoc` produces
/// `emissionQuant = meanBaseRate × starts` and `emissionQuantIM =
/// meanBaseRateIM × starts`; the I/M `update` then blends them per matched
/// `IMCoverageMergedUngrouped` row:
/// `max(emissionQuantIM × IMAdjustFract + emissionQuant × (1 −
/// IMAdjustFract), 0)`. A row with no I/M match keeps `emissionQuant`.
fn assemble_emission_output(
    inputs: &CriteriaStartInputs,
    ctx: &RunContext,
    starts2: &[Starts2],
    activity_weighted_2: &[ActivityWeightedEmissionRate2],
    im_merged: &[ImCoverageMerged],
) -> Vec<CriteriaStartEmissionRow> {
 // ActivityWeightedEmissionRate2 indexed for the seven-column join.
    let mut awer2_by_key: FxHashMap<[i32; 7], Vec<&ActivityWeightedEmissionRate2>> =
        FxHashMap::with_capacity_and_hasher(activity_weighted_2.len(), Default::default());
    for awer in activity_weighted_2 {
        awer2_by_key
            .entry([
                awer.zone_id,
                awer.month_id,
                awer.hour_id,
                awer.day_id,
                awer.year_id,
                awer.source_type_id,
                awer.model_year_id,
            ])
            .or_default()
            .push(awer);
    }
 // PollutantProcessAssoc lookup — polProcessID → (pollutantID, processID).
    let ppa: FxHashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
 // IMCoverageMergedUngrouped lookup for the I/M update — one row per key
 // (it is a GROUP BY result).
    let im_by_key: FxHashMap<[i32; 5], f64> = im_merged
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

    let mut out: Vec<CriteriaStartEmissionRow> = Vec::new();
    for s in starts2 {
 // INNER JOIN ActivityWeightedEmissionRate2 ON (zone, month, hour,
 // day, year, sourceType, modelYear).
        let Some(awers) = awer2_by_key.get(&[
            s.zone_id,
            s.month_id,
            s.hour_id,
            s.day_id,
            s.year_id,
            s.source_type_id,
            s.model_year_id,
        ]) else {
            continue;
        };
        for awer in awers {
 // INNER JOIN PollutantProcessAssoc USING (polProcessID).
            let Some(assoc) = ppa.get(&awer.pol_process_id) else {
                continue;
            };
            let emission_quant = awer.mean_base_rate * s.starts;
            let emission_quant_im = awer.mean_base_rate_im * s.starts;
 // I/M update — blend on a match, GREATEST-clamp at 0.
            let final_quant = match im_by_key.get(&[
                assoc.process_id,
                assoc.pollutant_id,
                s.model_year_id,
                awer.fuel_type_id,
                s.source_type_id,
            ]) {
                Some(&im_adjust_fract) => (emission_quant_im * im_adjust_fract
                    + emission_quant * (1.0 - im_adjust_fract))
                    .max(0.0),
                None => emission_quant,
            };
            out.push(CriteriaStartEmissionRow {
                state_id: ctx.state_id,
                county_id: ctx.county_id,
                zone_id: s.zone_id,
                link_id: ctx.link_id,
                road_type_id: OFF_NETWORK_ROAD_TYPE_ID,
                year_id: s.year_id,
                month_id: s.month_id,
                day_id: s.day_id,
                hour_id: s.hour_id,
                pollutant_id: assoc.pollutant_id,
                process_id: assoc.process_id,
                source_type_id: s.source_type_id,
                model_year_id: s.model_year_id,
                fuel_type_id: awer.fuel_type_id,
                emission_quant: final_quant,
            });
        }
    }
    out
}

/// The MOVES criteria-pollutant start-exhaust calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input
/// flows through the [`CriteriaStartInputs`] / [`RunContext`] arguments to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct CriteriaStartCalculator {
 /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl CriteriaStartCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Construct the calculator with its master-loop subscription.
 ///
 /// `CriteriaStartCalculator` is a `GenericCalculatorBase` whose
 /// constructor passes the `polProcessID`s `102`/`202`/`302` (THC, CO and
 /// NOx Start Exhaust) at `MONTH` granularity with a `0` priority offset.
 /// `GenericCalculatorBase.subscribeToMe` issues one `targetLoop.subscribe`
 /// per process — and all three pairs share the Start Exhaust process — so
 /// the calculator carries a single subscription: Start Exhaust at `MONTH`
 /// granularity, `EMISSION_CALCULATOR` priority. `calculator-dag.json`
 /// records the same lone subscription with an unresolved process id; this
 /// port resolves it to Start Exhaust.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                ProcessId(START_EXHAUST_PROCESS_ID),
                Granularity::Month,
                priority,
            )],
        }
    }

 /// Port of `doesProcessContext` — whether the master loop should run the
 /// calculator for a context with the given road type.
 ///
 /// Engine starts are modelled as off-network activity, so the calculator
 /// runs only for the off-network road type (`roadTypeID == 1`). The Java
 /// predicate returns `false` only when the road type is a positive,
 /// non-off-network id; an absent (`<= 0`) road type still passes. This is
 /// the master-loop context filter the `execute` wiring uses; the
 /// pure [`calculate`](Self::calculate) is correct for any input because
 /// the CSEC-8 output unconditionally stamps `roadTypeID = 1`.
    #[must_use]
    pub fn processes_context(road_type_id: i32) -> bool {
        !(road_type_id > 0 && road_type_id != OFF_NETWORK_ROAD_TYPE_ID)
    }

 /// Compute the criteria start-exhaust emission rows — the port of the
 /// `CriteriaStartCalculator.sql` "Processing" section.
 ///
 /// The CSEC steps run in order: CSEC 1-a merges the I/M coverage, CSEC 2
 /// builds the fuel-supply adjustment, CSEC-3 the start-temperature
 /// adjustment, CSEC-4 adds it to the per-bin emission rates, CSEC-5 weights
 /// them by source bin, CSEC-6 by operating mode, CSEC-7 applies the fuel
 /// adjustment, and CSEC-8 multiplies by start activity and applies the I/M
 /// blend. The result is sorted by its integer dimension columns for
 /// deterministic output — MOVES leaves `MOVESWorkerOutput` physically
 /// unordered.
    #[must_use]
    pub fn calculate(
        &self,
        inputs: &CriteriaStartInputs,
        ctx: &RunContext,
    ) -> Vec<CriteriaStartEmissionRow> {
 // CSEC 1-a (im_merged), CSEC-2 (fuel_supply_adj), CSEC-3 (met_start),
 // and the starts2 input for CSEC-8 are mutually independent — each
 // reads only `inputs`/`ctx`. Run all four in parallel so the Start
 // chunk exploits multiple cores instead of executing them serially.
        let ((im_merged, fuel_supply_adj), (met_start, starts2)) = join(
            || {
                join(
                    || im_coverage_merged(inputs, ctx),
                    || fuel_supply_adjustment(inputs, ctx),
                )
            },
            || {
                join(
                    || met_start_adjustment(inputs),
                    || build_starts2(inputs, ctx),
                )
            },
        );
 // CSEC-4 through CSEC-7 form a sequential chain: each step feeds
 // the next. CSEC-7 joins the chain output with `fuel_supply_adj`
 // (already computed by the parallel phase above).
        let emission_rates = emission_rates_with_im_and_temp(inputs, ctx, &met_start);
        let met_source_bin = met_source_bin_emission_rates(inputs, &emission_rates);
        let activity_weighted = activity_weighted_emission_rate(inputs, &met_source_bin);
        let activity_weighted_2 =
            activity_weighted_emission_rate_2(inputs, &activity_weighted, &fuel_supply_adj);
        let mut output =
            assemble_emission_output(inputs, ctx, &starts2, &activity_weighted_2, &im_merged);

        output.par_sort_unstable_by_key(CriteriaStartEmissionRow::dimension_key);
        output
    }
}

impl Default for CriteriaStartCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// `CriteriaStartCalculator` registers no `(pollutant, process)` pairs — see
/// [`Calculator::registrations`] on the impl below.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB / execution-DB tables the criteria start computation consumes/// the data tables the SQL's "Extract Data" section pulls and the "Processing"
/// section reads. The SQL also extracts `FuelType` and
/// `PollutantProcessModelYear`; neither feeds "Processing", so neither is
/// listed.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "County",
    "EmissionRateByAge",
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "MonthOfAnyYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
    "StartTempAdjustment",
    "Starts",
    "Year",
    "Zone",
    "ZoneMonthHour",
    "criteriaRatio",
];

impl Calculator for CriteriaStartCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

 /// `CriteriaStartCalculator` registers **no** `(pollutant, process)`
 /// pairs.
 ///
 /// The Java `GenericCalculatorBase.doRegistration` registers THC, CO and
 /// NOx Start Exhaust — but those are legacy registrations. In the pinned
 /// MOVES, `CalculatorInfo.txt` (the runtime registration file) has no
 /// `Registration` directive for this module: the base-rate approach
 /// (`BaseRateCalculator`,) carries the criteria start-exhaust
 /// pairs, and `calculator-dag.json` records `registrations_count: 0` to
 /// match.
 ///
 /// Returning an empty slice keeps this port consistent with the runtime
 /// and prevents the registry from double-registering against
 /// `BaseRateCalculator`. See the [module docs](self).
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

 // `upstream` keeps the trait default (empty): `calculator-dag.json`
 // records `depends_on: []`. `CriteriaStartCalculator` subscribes directly
 // to the master loop.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let fuel_supply_rows: Vec<FuelSupplyRow> = tables.iter_typed("FuelSupply")?;
        let fuel_region_id = fuel_supply_rows
            .first()
            .map(|r| r.fuel_region_id)
            .unwrap_or(0);
        let ppa_rows: Vec<PollutantProcessAssocRow> = tables.iter_typed("PollutantProcessAssoc")?;
        let pol_process_ids: Vec<i32> = ppa_rows.iter().map(|r| r.pol_process_id).collect();
        let run_ctx = RunContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            fuel_region_id,
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            pol_process_ids,
        };
        let inputs = CriteriaStartInputs {
            age_category: tables.iter_typed("AgeCategory")?,
            county: tables.iter_typed("County")?,
            criteria_ratio: tables.iter_typed("criteriaRatio")?,
            emission_rate_by_age: tables.iter_typed("EmissionRateByAge")?,
            fuel_formulation: tables.iter_typed("FuelFormulation")?,
            fuel_subtype: tables.iter_typed("FuelSubtype")?,
            fuel_supply: fuel_supply_rows,
            hour_day: tables.iter_typed("HourDay")?,
            im_coverage: tables.iter_typed("IMCoverage")?,
            im_factor: tables.iter_typed("IMFactor")?,
            month_of_any_year: tables.iter_typed("MonthOfAnyYear")?,
            op_mode_distribution: tables.iter_typed("OpModeDistribution")?,
            pollutant_process_assoc: ppa_rows,
            pollutant_process_mapped_model_year: tables
                .iter_typed("PollutantProcessMappedModelYear")?,
            source_bin: tables.iter_typed("SourceBin")?,
            source_bin_distribution: tables.iter_typed("SourceBinDistribution")?,
            source_type_model_year: tables.iter_typed("SourceTypeModelYear")?,
            start_temp_adjustment: tables.iter_typed("StartTempAdjustment")?,
            starts: tables.iter_typed("Starts")?,
            year: tables.iter_typed("Year")?,
            zone: tables.iter_typed("Zone")?,
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
    Box::new(CriteriaStartCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

 /// CO Start Exhaust `polProcessID` — `pollutantID 2 × 100 + processID 2`.
    const CO_START_POL_PROCESS: i32 = 202;

 /// The run context the test fixtures use: calendar year 2020, fuel region
 /// 100, county 26161, link 5001, state 26, processing CO Start Exhaust.
    fn run_context() -> RunContext {
        RunContext {
            year: 2020,
            fuel_region_id: 100,
            county_id: 26_161,
            link_id: 5001,
            state_id: 26,
            pol_process_ids: vec![CO_START_POL_PROCESS],
        }
    }

 /// A minimal one-of-everything input that threads exactly one row through
 /// all CSEC steps.
 ///
 /// Hand-computed with the default `temperature` of `75.0` (zero
 /// temperature adjustment): CSEC 1-a `IMAdjustFract = 1.0 × 50.0 × 0.01 =
 /// 0.5`; CSEC 2 `fuelAdjustment = 3.0 + 0.0 × (9.0 − 3.0) = 3.0`,
 /// market-share-weighted to `3.0 × 1.0 = 3.0`; CSEC-4 `meanBaseRate =
 /// 10.0 + 0.0 = 10.0`, `meanBaseRateIM = 4.0 + 0.0 = 4.0`; CSEC-5/6 carry
 /// the single bin/op-mode unchanged; CSEC-7 `meanBaseRate = 10.0 × 3.0 =
 /// 30.0`, `meanBaseRateIM = 4.0 × 3.0 = 12.0`; CSEC-8 `emissionQuant =
 /// 30.0 × 100.0 = 3000.0`, `emissionQuantIM = 12.0 × 100.0 = 1200.0`; I/M
 /// blend `max(1200.0 × 0.5 + 3000.0 × 0.5, 0) = 2100.0`.
    fn minimal_inputs() -> CriteriaStartInputs {
        CriteriaStartInputs {
            age_category: vec![AgeCategoryRow {
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                age_group_id: 300,
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                gpa_fract: 0.0,
            }],
            criteria_ratio: vec![CriteriaRatioRow {
                pol_process_id: CO_START_POL_PROCESS,
                fuel_formulation_id: 100,
                source_type_id: 21,
                model_year_id: 2018,
                ratio: 3.0,
                ratio_gpa: 9.0,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 5000,
                pol_process_id: CO_START_POL_PROCESS,
                op_mode_id: 108,
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
                fuel_region_id: 100,
                fuel_year_id: 2020,
                month_group_id: 7,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            im_coverage: vec![ImCoverageRow {
                pol_process_id: CO_START_POL_PROCESS,
                source_type_id: 21,
                fuel_type_id: 1,
                beg_model_year_id: 2010,
                end_model_year_id: 2020,
                inspect_freq: 1,
                test_standards_id: 1,
                compliance_factor: 50.0,
            }],
            im_factor: vec![ImFactorRow {
                pol_process_id: CO_START_POL_PROCESS,
                inspect_freq: 1,
                test_standards_id: 1,
                source_type_id: 21,
                fuel_type_id: 1,
                im_model_year_group_id: 500,
                age_group_id: 300,
                im_factor: 1.0,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 7,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 85,
                pol_process_id: CO_START_POL_PROCESS,
                op_mode_id: 108,
                op_mode_fraction: 1.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: CO_START_POL_PROCESS,
                pollutant_id: 2,
                process_id: 2,
            }],
            pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
                pol_process_id: CO_START_POL_PROCESS,
                model_year_id: 2018,
                model_year_group_id: 400,
                im_model_year_group_id: 500,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 5000,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: CO_START_POL_PROCESS,
                source_bin_id: 5000,
                source_bin_activity_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                source_type_id: 21,
                model_year_id: 2018,
            }],
            start_temp_adjustment: vec![StartTempAdjustmentRow {
                pol_process_id: CO_START_POL_PROCESS,
                model_year_group_id: 400,
                fuel_type_id: 1,
                op_mode_id: 108,
                equation_type: "POLY".to_string(),
                term_a: 0.1,
                term_b: 0.0,
                term_c: 0.0,
            }],
            starts: vec![StartsRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2,
                zone_id: 90,
                source_type_id: 21,
                starts: 100.0,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            zone: vec![ZoneRow {
                zone_id: 90,
                county_id: 26_161,
            }],
            zone_month_hour: vec![ZoneMonthHourRow {
                zone_id: 90,
                month_id: 7,
                hour_id: 8,
                temperature: 75.0, // at the reference temperature → zero adjustment
            }],
        }
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
        let calc = CriteriaStartCalculator::new();
        let rows = calc.calculate(&minimal_inputs(), &run_context());
        assert_eq!(rows.len(), 1);
        let row = rows[0];
        assert_quant(row.emission_quant, 2100.0);
        assert_eq!(row.state_id, 26);
        assert_eq!(row.county_id, 26_161);
        assert_eq!(row.zone_id, 90);
        assert_eq!(row.link_id, 5001);
        assert_eq!(row.road_type_id, 1);
        assert_eq!(row.year_id, 2020);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 8);
        assert_eq!(row.pollutant_id, 2);
        assert_eq!(row.process_id, 2);
        assert_eq!(row.source_type_id, 21);
        assert_eq!(row.model_year_id, 2018);
        assert_eq!(row.fuel_type_id, 1);
    }

    #[test]
    fn calculate_applies_the_polynomial_temperature_adjustment() {
 // temperature 50 → d = min(50, 75) - 75 = -25; POLY adjustment =
 // -25 × (0.1 + -25 × (0 + -25 × 0)) = -25 × 0.1 = -2.5.
 // meanBaseRate = 10 - 2.5 = 7.5, meanBaseRateIM = 4 - 2.5 = 1.5.
 // CSEC-7 ×3 → 22.5 / 4.5; CSEC-8 ×100 → 2250 / 450;
 // I/M blend max(450 × 0.5 + 2250 × 0.5, 0) = 1350.
        let mut inputs = minimal_inputs();
        inputs.zone_month_hour[0].temperature = 50.0;
        let rows = CriteriaStartCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 1350.0);
    }

    #[test]
    fn calculate_applies_the_logarithmic_temperature_adjustment() {
 // LOG adjustment = term_b × exp(term_a × d) + term_c. With term_a = 0
 // exp(...) = 1, so adjustment = term_b = 2.0 regardless of d.
 // meanBaseRate = 10 + 2 = 12, meanBaseRateIM = 4 + 2 = 6.
 // CSEC-7 ×3 → 36 / 18; CSEC-8 ×100 → 3600 / 1800;
 // I/M blend max(1800 × 0.5 + 3600 × 0.5, 0) = 2700.
        let mut inputs = minimal_inputs();
        inputs.zone_month_hour[0].temperature = 50.0;
        inputs.start_temp_adjustment[0].equation_type = "log".to_string(); // case-insensitive
        inputs.start_temp_adjustment[0].term_a = 0.0;
        inputs.start_temp_adjustment[0].term_b = 2.0;
        inputs.start_temp_adjustment[0].term_c = 0.0;
        let rows = CriteriaStartCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 2700.0);
    }

    #[test]
    fn calculate_without_im_coverage_leaves_emission_unadjusted() {
 // No IMFactor / IMCoverage → no IMCoverageMergedUngrouped row → the
 // I/M update finds no match, so emissionQuant = meanBaseRate × starts
 // = 30 × 100 = 3000 (no blend with the I/M rate).
        let mut inputs = minimal_inputs();
        inputs.im_factor.clear();
        inputs.im_coverage.clear();
        let rows = CriteriaStartCalculator::new().calculate(&inputs, &run_context());
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
        let rows = CriteriaStartCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 0.0);
    }

    #[test]
    fn calculate_weights_emission_rates_across_source_bins() {
 // Two source bins, both fuel type 1 / op mode 108, with activity
 // fractions 0.6 and 0.4. CSEC-5 sums: meanBaseRate =
 // 10 × 0.6 + 20 × 0.4 = 14. CSEC-7 ×3 → 42; CSEC-8 ×100 → 4200
 // emissionQuant. No I/M coverage, so emission = 4200.
        let mut inputs = minimal_inputs();
        inputs.im_factor.clear();
        inputs.im_coverage.clear();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 5001,
            fuel_type_id: 1,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 5001,
            pol_process_id: CO_START_POL_PROCESS,
            op_mode_id: 108,
            age_group_id: 300,
            mean_base_rate: 20.0,
            mean_base_rate_im: 20.0,
        });
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.6;
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: CO_START_POL_PROCESS,
                source_bin_id: 5001,
                source_bin_activity_fraction: 0.4,
            });
        let rows = CriteriaStartCalculator::new().calculate(&inputs, &run_context());
        assert_eq!(rows.len(), 1);
        assert_quant(rows[0].emission_quant, 4200.0);
    }

    #[test]
    fn calculate_empty_inputs_yields_no_rows() {
        let calc = CriteriaStartCalculator::new();
        let rows = calc.calculate(&CriteriaStartInputs::default(), &run_context());
        assert!(rows.is_empty());
    }

    #[test]
    fn processes_context_runs_only_off_network() {
 // Off-network and absent road types pass; positive on-network ids do
 // not — engine starts are modelled as off-network activity.
        assert!(CriteriaStartCalculator::processes_context(1)); // off-network
        assert!(CriteriaStartCalculator::processes_context(0)); // absent
        assert!(CriteriaStartCalculator::processes_context(-1)); // absent
        assert!(!CriteriaStartCalculator::processes_context(2)); // rural restricted
        assert!(!CriteriaStartCalculator::processes_context(5)); // urban unrestricted
    }

    #[test]
    fn calculator_metadata_matches_the_runtime() {
        let calc = CriteriaStartCalculator::new();
        assert_eq!(calc.name(), "CriteriaStartCalculator");

 // One subscription: Start Exhaust, MONTH, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(2));
        assert_eq!(subs[0].granularity, Granularity::Month);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

 // Superseded by BaseRateCalculator — no registrations.
        assert!(calc.registrations().is_empty());
 // Subscribes directly — no upstream chain dependency.
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"StartTempAdjustment"));
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CriteriaStartCalculator");
        let calcs: Vec<Box<dyn Calculator>> = vec![factory()];
        assert_eq!(calcs[0].name(), "CriteriaStartCalculator");
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "AgeCategory",
            AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap(),
        );
        store.insert(
            "County",
            CountyRow::into_dataframe(inputs.county.clone()).unwrap(),
        );
        store.insert(
            "criteriaRatio",
            CriteriaRatioRow::into_dataframe(inputs.criteria_ratio.clone()).unwrap(),
        );
        store.insert(
            "EmissionRateByAge",
            EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age.clone()).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation.clone()).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubtypeRow::into_dataframe(inputs.fuel_subtype.clone()).unwrap(),
        );
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply.clone()).unwrap(),
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
            "PollutantProcessMappedModelYear",
            PollutantProcessMappedModelYearRow::into_dataframe(
                inputs.pollutant_process_mapped_model_year.clone(),
            )
            .unwrap(),
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
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap(),
        );
        store.insert(
            "StartTempAdjustment",
            StartTempAdjustmentRow::into_dataframe(inputs.start_temp_adjustment.clone()).unwrap(),
        );
        store.insert(
            "Starts",
            StartsRow::into_dataframe(inputs.starts.clone()).unwrap(),
        );
        store.insert(
            "Year",
            YearRow::into_dataframe(inputs.year.clone()).unwrap(),
        );
        store.insert(
            "Zone",
            ZoneRow::into_dataframe(inputs.zone.clone()).unwrap(),
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
        let calc = CriteriaStartCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(
            out.dataframe().unwrap().height() > 0,
            "expected at least one row"
        );
    }
}
