//! `BasicRunningPMEmissionCalculator` — the running-exhaust-PM half of Phase 3
//! Task 53 (the total-exhaust-PM half is [`super::total`]).
//!
//! Pure-Rust port of `BasicRunningPMEmissionCalculator.java` and the
//! processing pipeline of `database/BasicPM25Calculator.sql`.
//!
//! # What this calculator does
//!
//! `BasicRunningPMEmissionCalculator` produces the two PM2.5 exhaust
//! components for the **Running Exhaust** process (process 1):
//!
//! * elemental carbon PM2.5 — pollutant 112;
//! * non-elemental-carbon PM2.5 — pollutant 118 (`Composite - NonECPM`).
//!
//! It follows the classic *activity × emission rate* methodology: a base
//! emission rate is weighted by the operating-mode distribution and the
//! source-bin distribution, multiplied by source-hours-operating activity,
//! adjusted for fuel effects, and adjusted for temperature.
//!
//! # `GenericCalculatorBase` and the script-section selection
//!
//! The Java class is a thin subclass of `GenericCalculatorBase`. Its whole
//! body is the constructor, which passes five arguments up:
//!
//! ```text
//! super(new String[] { "11801", "11201" },          // polProcessIDs
//!       MasterLoopGranularity.YEAR,                  // granularity
//!       0,                                           // priority offset
//!       "database/BasicPM25Calculator.sql",          // script
//!       new String[]{ "HasManyOpModes", "EmissionRateByAgeRates",
//!                     "SourceHoursOperatingActivity",
//!                     "ApplyTemperatureAdjustment" });// enabled sections
//! ```
//!
//! `BasicPM25Calculator.sql` is a *generic template*: it carries mutually
//! exclusive variants of each step gated behind `-- Section …` markers
//! (`HasManyOpModes` vs `HasOneOpMode`, `EmissionRateByAgeRates` vs
//! `EmissionRateRates`, three activity variants, four temperature variants).
//! The constructor's `additionalSectionNames` argument selects exactly one
//! configuration. This port implements **only** that configuration — the
//! single path `BasicRunningPMEmissionCalculator` exercises — not the generic
//! template. The other section variants belong to sibling calculators and to
//! `BasicStartPMEmissionCalculator` (migration-plan Task 54, a separate
//! script `BasicStartPM25Calculator.sql`).
//!
//! The `polProcessID` strings encode `pollutantID * 100 + processID`:
//! `11801` → pollutant 118, process 1; `11201` → pollutant 112, process 1.
//! Both are Running Exhaust, which is why the calculator subscribes to a
//! single process.
//!
//! # The pipeline — `BasicPM25Calculator.sql` "Processing" section
//!
//! The script's processing section runs six steps, labelled "BRPMC Step N"
//! in the SQL. With this calculator's section selection they are:
//!
//! 1. **Weight emission rates by operating mode** (`step1_op_mode_weighted`).
//!    `EmissionRateByAge.meanBaseRate` is multiplied by
//!    `OpModeDistribution.opModeFraction` and summed over operating mode,
//!    keyed by `(hourDay, sourceType, sourceBin, ageGroup, polProcess)`.
//! 2. **Weight by source bin** (`step2_fully_weighted`). The op-mode-weighted
//!    rate is multiplied by `SourceBinDistribution.sourceBinActivityFraction`
//!    and summed over source bin, resolving the `(fuelType, modelYear, age)`
//!    dimensions.
//! 3. **Multiply by activity** (`step3_unadjusted`). The fully weighted rate
//!    is multiplied by `SHO.SHO` (source-hours operating) to give an
//!    unadjusted emission quantity.
//! 4. **Apply the fuel adjustment** (`step4_fuel_adjusted`). A
//!    market-share-weighted fuel-effect ratio (`generalFuelRatio`, blended
//!    GPA / non-GPA) scales the emission quantity.
//! 5. **Apply the temperature adjustment** (`step5_temperature_adjusted`).
//!    Below 72 °F the quantity is scaled by `exp(tempAdjustTermA·(72−T))`.
//! 6. **Convert to worker-output rows** (`step6_worker_output`). The
//!    `polProcessID` is split back into `(pollutantID, processID)` and the
//!    iteration geography is attached.
//!
//! The script's "Create Remote Tables" / "Extract Data" / "Cleanup" sections
//! are MariaDB I/O boilerplate — they load the per-iteration filtered tables
//! and drop the temporaries. The Rust port receives those tables already
//! materialised as [`BasicRunningPmInputs`]; only the computation is ported.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` intermediate columns.** Every "BRPMC Step" writes its result
//!   into a `FLOAT` (32-bit) temp column while MariaDB evaluates the
//!   arithmetic in `DOUBLE`; a value therefore truncates to `f32` between
//!   steps. This port computes in [`f64`] throughout, matching the precedent
//!   set by the generator ports (migration-plan Tasks 33 and 41): the
//!   ~1e-7-relative truncation drift is left for the Task 44 / Task 117
//!   fidelity gate to rule bug-compatible or not against canonical captures.
//! * **`SELECT DISTINCT` in Steps 1 and 4.** Step 1 selects six columns
//!   (`hourDay, sourceType, sourceBin, ageGroup, polProcess`, and the weighted
//!   rate) from a join that also threads `SourceBinDistribution`,
//!   `AgeCategory` and `SourceTypeModelYear` — tables whose columns are *not*
//!   selected. A single `(opMode, rate)` pair can satisfy the join through
//!   several source-bin / model-year chains, producing duplicate output rows;
//!   `SELECT DISTINCT` collapses them so the pair is summed once. The port
//!   reproduces this by de-duplicating the exact emitted tuples (the rate
//!   compared by its `f64` bit pattern) before the group-and-sum. Step 4-c's
//!   `SELECT DISTINCT` is reproduced the same way.
//! * **Cross joins.** `BRPMC Step 4-b`'s `FuelSupplyAdjustment` query and
//!   `BRPMC Step 6` use MariaDB `INNER JOIN`s with no `ON` clause — i.e.
//!   cross joins. The port iterates the cartesian product directly. In
//!   `Step 6` the cross-joined `Link` table is extract-filtered to the
//!   iteration's single link, so it normally contributes exactly one row.
//! * **Sum order.** Steps 1, 2 and 4-b sum `f64` values; the SQL `GROUP BY`
//!   carries `ORDER BY NULL`, leaving the accumulation order undefined. The
//!   port accumulates in input-row order, which is deterministic for a fixed
//!   input. Any divergence from MariaDB's order is within the tolerance the
//!   migration plan already accepts for unordered floating-point sums.
//!
//! # Chain metadata — a superseded calculator
//!
//! `BasicRunningPMEmissionCalculator` is a **legacy calculator superseded by
//! `BaseRateCalculator`** (migration-plan Task 45). The modern base-rate
//! methodology produces the same running-exhaust PM2.5 output, so a current
//! MOVES run wires `BaseRateCalculator`, not this calculator: its
//! `ModuleName` is absent from `CalculatorInfo.txt`, and
//! `characterization/calculator-chains/calculator-dag.json` records it with
//! `registrations_count: 0` and `depends_on: []`.
//!
//! Consequently [`registrations`](Calculator::registrations) returns an
//! empty slice — pollutants 112 and 118 for Running Exhaust are registered
//! to `BaseRateCalculator`, and returning them here too would double-register
//! the pairs. The DAG still records a single subscription —
//! `subscribes_directly: true`, granularity `YEAR`, priority
//! `EMISSION_CALCULATOR` — but with a placeholder `process_id` of `0`, since
//! the static analyser could see the Java but not resolve the `polProcessID`
//! strings to a process. [`subscriptions`](Calculator::subscriptions)
//! resolves them: both `11801` and `11201` decode to process 1. See each
//! trait method's doc comment for the derivation.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] is a shell: its [`CalculatorContext`] exposes only
//! the Phase 2 placeholder `ExecutionTables` / `ScratchNamespace`, which have
//! no row storage. The faithful pipeline is
//! [`BasicRunningPmEmissionCalculator::run`], fully unit-tested. Once the
//! `DataFrameStore` (migration-plan Task 50) lands, `execute` materialises a
//! [`BasicRunningPmInputs`] and a [`RunContext`] from the context, calls
//! `run`, and writes the [`MovesWorkerOutputRow`]s back.

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name in the calculator-chain DAG — matches the Java class.
const CALCULATOR_NAME: &str = "BasicRunningPMEmissionCalculator";

/// Running Exhaust — the one process this calculator covers. Both Java
/// `polProcessID` strings (`11801`, `11201`) decode to it.
const RUNNING_EXHAUST_PROCESS_ID: u16 = 1;

/// `BRPMC Step 5` reference temperature, in °F: at or below it the
/// temperature adjustment scales emissions up, above it the adjustment is the
/// identity.
const REFERENCE_TEMPERATURE_F: f64 = 72.0;

// ---------------------------------------------------------------------------
// Input tables
//
// One struct per default-database table the processing section consumes,
// carrying only the columns the six steps read. Field names mirror the SQL
// column names in `snake_case`; all id columns are held as `i32` (`sourceBinID`
// is a SQL `BIGINT`, held as `i64`).
// ---------------------------------------------------------------------------

/// One `OpModeDistribution` row — the operating-mode fractions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `opModeFraction` — the fraction of activity in this operating mode.
    pub op_mode_fraction: f64,
}

/// One `EmissionRateByAge` row — the age-resolved base emission rates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `ageGroupID`.
    pub age_group_id: i32,
    /// `meanBaseRate` — the base emission rate.
    pub mean_base_rate: f64,
}

/// One `SourceBinDistribution` row — the source-bin activity fractions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `sourceTypeModelYearID`.
    pub source_type_model_year_id: i32,
    /// `sourceBinActivityFraction` — the fraction of activity in this bin.
    pub source_bin_activity_fraction: f64,
}

/// One `AgeCategory` row — maps an age group to its representative age.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageGroupID`.
    pub age_group_id: i32,
    /// `ageID` — the age in years; `modelYear = runYear − ageID`.
    pub age_id: i32,
}

/// One `SourceTypeModelYear` row — maps a source-type-model-year surrogate
/// key to its source type and model year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID`.
    pub source_type_model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
}

/// One `PollutantProcessModelYear` row — the model-year-group mapping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
}

/// One `SourceBin` row — maps a source bin to its fuel type and model-year
/// group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
}

/// One `SHO` row — source-hours-operating activity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `SHO` — the source-hours-operating quantity.
    pub sho: f64,
}

/// One `FuelSupply` row — the per-fuel-formulation market shares.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
    /// `monthGroupID`.
    pub month_group_id: i32,
    /// `marketShare` — the formulation's share of the supply.
    pub market_share: f64,
}

/// One `FuelFormulation` row — maps a formulation to its fuel subtype.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
}

/// One `FuelSubType` row — maps a fuel subtype to its fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubTypeRow {
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
}

/// One `MonthOfAnyYear` row — maps a month to its month group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthOfAnyYearRow {
    /// `monthID`.
    pub month_id: i32,
    /// `monthGroupID`.
    pub month_group_id: i32,
}

/// One `Year` row — maps a calendar year to its fuel year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID`.
    pub year_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
}

/// One `County` row — carries the geographic-phase-in (GPA) fraction the fuel
/// adjustment blends GPA and non-GPA fuel-effect ratios with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID`.
    pub county_id: i32,
    /// `GPAFract` — the fraction of the county inside the GPA region.
    pub gpa_fract: f64,
}

/// One `RunSpecSourceType` row — a source type selected by the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `generalFuelRatio` row — the fuel-effect ratios applied in
/// `BRPMC Step 4`. A row matches a `(formulation, polProcess, modelYear, age,
/// sourceType)` combination through its model-year and age ranges.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeneralFuelRatioRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year range.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year range.
    pub max_model_year_id: i32,
    /// `minAgeID` — inclusive lower bound of the age range.
    pub min_age_id: i32,
    /// `maxAgeID` — inclusive upper bound of the age range.
    pub max_age_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelEffectRatio` — the non-GPA fuel-effect ratio; `None` (SQL `NULL`)
    /// is treated as `1` by the `ifnull(…, 1)` in the SQL.
    pub fuel_effect_ratio: Option<f64>,
    /// `fuelEffectRatioGPA` — the GPA fuel-effect ratio; `None` (SQL `NULL`)
    /// is likewise treated as `1`.
    pub fuel_effect_ratio_gpa: Option<f64>,
}

/// One `HourDay` row — splits an hour-day surrogate into its day and hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
}

/// One `ZoneMonthHour` row — the per-(month, hour) temperature for the
/// iteration's zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `monthID`.
    pub month_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `temperature` — the ambient temperature, °F.
    pub temperature: f64,
}

/// One `TemperatureAdjustment` row — the `BRPMC Step 5` exponential
/// temperature-correction term.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TemperatureAdjustmentRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year range.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year range.
    pub max_model_year_id: i32,
    /// `tempAdjustTermA` — the exponential coefficient. `None` (SQL `NULL`)
    /// makes the `exp(…)` `NULL`, which the SQL `coalesce` falls back from to
    /// the unadjusted quantity. The `ApplyLinearTemperatureAdjustment`
    /// section's `tempAdjustTermB` is not modelled — that section is not in
    /// this calculator's selection.
    pub temp_adjust_term_a: Option<f64>,
}

/// One `PollutantProcessAssoc` row — splits a `polProcessID` back into its
/// pollutant and process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
}

/// One `Link` row — carries the road type stamped onto the output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
}

/// The run constants the SQL reads as `##context.…##` substitutions — the
/// calendar year and the iteration geography.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunContext {
    /// `##context.year##` — the run's calendar year. `modelYear` is derived
    /// from it as `year − age`.
    pub year: i32,
    /// `##context.iterLocation.stateRecordID##` — stamped onto the output.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##` — stamped onto the output
    /// and used as the `FuelSupplyWithFuelType.countyID` literal.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##` — stamped onto the output.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##` — stamped onto the output.
    pub link_id: i32,
}

/// Every input table the processing pipeline consumes — the in-memory
/// equivalent of the per-iteration filtered MariaDB execution database the
/// script's "Extract Data" section would have loaded.
#[derive(Debug, Clone, Default)]
pub struct BasicRunningPmInputs {
    /// `OpModeDistribution`.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `EmissionRateByAge`.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `SourceBinDistribution`.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `AgeCategory`.
    pub age_category: Vec<AgeCategoryRow>,
    /// `SourceTypeModelYear`.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `PollutantProcessModelYear`.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `SourceBin`.
    pub source_bin: Vec<SourceBinRow>,
    /// `SHO` — source-hours-operating activity.
    pub sho: Vec<ShoRow>,
    /// `FuelSupply`.
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FuelFormulation`.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubType`.
    pub fuel_sub_type: Vec<FuelSubTypeRow>,
    /// `MonthOfAnyYear`.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `Year`.
    pub year: Vec<YearRow>,
    /// `County`.
    pub county: Vec<CountyRow>,
    /// `RunSpecSourceType`.
    pub run_spec_source_type: Vec<RunSpecSourceTypeRow>,
    /// `generalFuelRatio`.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
    /// `HourDay`.
    pub hour_day: Vec<HourDayRow>,
    /// `ZoneMonthHour`.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
    /// `TemperatureAdjustment`.
    pub temperature_adjustment: Vec<TemperatureAdjustmentRow>,
    /// `PollutantProcessAssoc`.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `Link`.
    pub link: Vec<LinkRow>,
}

/// One emission row in the structure of `MOVESWorkerOutput` — the result of
/// `BRPMC Step 6` and the value [`BasicRunningPmEmissionCalculator::run`]
/// produces.
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
    /// `pollutantID` — 112 (elemental carbon) or 118 (composite non-EC).
    pub pollutant_id: i32,
    /// `processID` — always 1 (Running Exhaust) for this calculator.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — the temperature- and fuel-adjusted emission quantity.
    pub emission_quant: f64,
}

// ---------------------------------------------------------------------------
// Intermediate tables — the SQL `BRPMC` temp tables, one private struct each.
// ---------------------------------------------------------------------------

/// `OpModeWeightedEmissionRate` — the `BRPMC Step 1` result.
#[derive(Debug, Clone, Copy)]
struct OpModeWeightedRate {
    hour_day_id: i32,
    source_type_id: i32,
    source_bin_id: i64,
    age_group_id: i32,
    pol_process_id: i32,
    op_mode_weighted_mean_base_rate: f64,
}

/// `FullyWeightedEmissionRate` — the `BRPMC Step 2` result.
#[derive(Debug, Clone, Copy)]
struct FullyWeightedRate {
    year_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    fully_weighted_mean_base_rate: f64,
    age_id: i32,
}

/// `UnadjustedEmissionResults` — the `BRPMC Step 3` result.
#[derive(Debug, Clone, Copy)]
struct UnadjustedEmission {
    year_id: i32,
    month_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    unadjusted_emission_quant: f64,
}

/// `FuelSupplyWithFuelType` — the `BRPMC Step 4-b` intermediate that pairs
/// each fuel formulation's market share with its fuel type.
///
/// The SQL table also carries a `countyID` column set to the iteration's
/// county, but the downstream `FuelSupplyAdjustment` query groups by the
/// `County` table's `countyID` instead, never reading `fsft.countyID`; it is
/// a dead column in the script and is not modelled here.
#[derive(Debug, Clone, Copy)]
struct FuelSupplyWithFuelType {
    year_id: i32,
    month_id: i32,
    fuel_formulation_id: i32,
    fuel_type_id: i32,
    market_share: f64,
}

/// `FuelSupplyAdjustment` — the `BRPMC Step 4-b` per-fuel-type adjustment
/// factor.
#[derive(Debug, Clone, Copy)]
struct FuelSupplyAdjustment {
    year_id: i32,
    month_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    fuel_adjustment: f64,
}

/// `FuelAdjustedEmissionRate` — the `BRPMC Step 4-c` result. The SQL keeps
/// the column name `unadjustedEmissionQuant` even though the value has had
/// the fuel adjustment applied; this field is the post-adjustment quantity.
#[derive(Debug, Clone, Copy)]
struct FuelAdjustedEmission {
    year_id: i32,
    month_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    emission_quant: f64,
}

/// `AdjustedEmissionResults` — the `BRPMC Step 5` result.
#[derive(Debug, Clone, Copy)]
struct AdjustedEmission {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    emission_quant: f64,
}

// ---------------------------------------------------------------------------
// Pipeline steps
// ---------------------------------------------------------------------------

/// `ifnull(x, 1)` — the SQL coalesce-to-one the fuel-effect ratios use.
fn ifnull_one(value: Option<f64>) -> f64 {
    value.unwrap_or(1.0)
}

/// `BRPMC Step 1` — weight emission rates by operating mode
/// (`EmissionRateByAgeRates` + `HasManyOpModes` section path).
///
/// Joins `OpModeDistribution` to `EmissionRateByAge` on `(polProcessID,
/// opModeID)`, threading `SourceBinDistribution`, `AgeCategory` and
/// `SourceTypeModelYear` as an existence filter (their columns are not
/// selected). `SELECT DISTINCT` collapses the join multiplicity, then the
/// per-operating-mode `opModeFraction · meanBaseRate` products are summed by
/// `(hourDay, sourceType, sourceBin, ageGroup, polProcess)`.
fn step1_op_mode_weighted(inputs: &BasicRunningPmInputs, year: i32) -> Vec<OpModeWeightedRate> {
    /// Group key — every dimension the sum is bucketed by.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Key {
        hour_day_id: i32,
        source_type_id: i32,
        source_bin_id: i64,
        age_group_id: i32,
        pol_process_id: i32,
    }

    let mut seen: HashSet<(i32, i32, i64, i32, i32, u64)> = HashSet::new();
    let mut weighted: BTreeMap<Key, f64> = BTreeMap::new();

    for omd in &inputs.op_mode_distribution {
        for er in &inputs.emission_rate_by_age {
            if er.pol_process_id != omd.pol_process_id || er.op_mode_id != omd.op_mode_id {
                continue;
            }
            for sbd in &inputs.source_bin_distribution {
                if sbd.pol_process_id != er.pol_process_id || sbd.source_bin_id != er.source_bin_id
                {
                    continue;
                }
                for acat in &inputs.age_category {
                    if acat.age_group_id != er.age_group_id {
                        continue;
                    }
                    for stmy in &inputs.source_type_model_year {
                        if stmy.source_type_model_year_id != sbd.source_type_model_year_id
                            || stmy.source_type_id != omd.source_type_id
                            || stmy.model_year_id != year - acat.age_id
                        {
                            continue;
                        }
                        let value = omd.op_mode_fraction * er.mean_base_rate;
                        let key = Key {
                            hour_day_id: omd.hour_day_id,
                            source_type_id: omd.source_type_id,
                            source_bin_id: er.source_bin_id,
                            age_group_id: er.age_group_id,
                            pol_process_id: omd.pol_process_id,
                        };
                        // `SELECT DISTINCT`: a join chain that re-emits the
                        // exact same six-column row contributes once.
                        if seen.insert((
                            key.hour_day_id,
                            key.source_type_id,
                            key.source_bin_id,
                            key.age_group_id,
                            key.pol_process_id,
                            value.to_bits(),
                        )) {
                            *weighted.entry(key).or_insert(0.0) += value;
                        }
                    }
                }
            }
        }
    }

    weighted
        .into_iter()
        .map(|(key, rate)| OpModeWeightedRate {
            hour_day_id: key.hour_day_id,
            source_type_id: key.source_type_id,
            source_bin_id: key.source_bin_id,
            age_group_id: key.age_group_id,
            pol_process_id: key.pol_process_id,
            op_mode_weighted_mean_base_rate: rate,
        })
        .collect()
}

/// `BRPMC Step 2` — weight emission rates by source bin.
///
/// Joins the `BRPMC Step 1` rate to `SourceBinDistribution`, `AgeCategory`,
/// `SourceTypeModelYear`, `PollutantProcessModelYear` and `SourceBin`, then
/// sums `sourceBinActivityFraction · opModeWeightedMeanBaseRate` by
/// `(hourDay, sourceType, fuelType, modelYear, polProcess, age)`.
fn step2_fully_weighted(
    inputs: &BasicRunningPmInputs,
    year: i32,
    op_mode_weighted: &[OpModeWeightedRate],
) -> Vec<FullyWeightedRate> {
    /// Group key for the source-bin weighting sum.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Key {
        hour_day_id: i32,
        source_type_id: i32,
        fuel_type_id: i32,
        model_year_id: i32,
        pol_process_id: i32,
        age_id: i32,
    }

    let mut weighted: BTreeMap<Key, f64> = BTreeMap::new();

    for omer in op_mode_weighted {
        for sbd in &inputs.source_bin_distribution {
            if sbd.source_bin_id != omer.source_bin_id || sbd.pol_process_id != omer.pol_process_id
            {
                continue;
            }
            for acat in &inputs.age_category {
                if acat.age_group_id != omer.age_group_id {
                    continue;
                }
                for stmy in &inputs.source_type_model_year {
                    if stmy.source_type_model_year_id != sbd.source_type_model_year_id
                        || stmy.source_type_id != omer.source_type_id
                        || stmy.model_year_id != year - acat.age_id
                    {
                        continue;
                    }
                    for ppmy in &inputs.pollutant_process_model_year {
                        if ppmy.pol_process_id != sbd.pol_process_id
                            || ppmy.model_year_id != stmy.model_year_id
                        {
                            continue;
                        }
                        for sb in &inputs.source_bin {
                            if sb.source_bin_id != sbd.source_bin_id
                                || sb.model_year_group_id != ppmy.model_year_group_id
                            {
                                continue;
                            }
                            let value = sbd.source_bin_activity_fraction
                                * omer.op_mode_weighted_mean_base_rate;
                            let key = Key {
                                hour_day_id: omer.hour_day_id,
                                source_type_id: omer.source_type_id,
                                fuel_type_id: sb.fuel_type_id,
                                model_year_id: stmy.model_year_id,
                                pol_process_id: omer.pol_process_id,
                                age_id: acat.age_id,
                            };
                            *weighted.entry(key).or_insert(0.0) += value;
                        }
                    }
                }
            }
        }
    }

    weighted
        .into_iter()
        .map(|(key, rate)| FullyWeightedRate {
            year_id: year,
            hour_day_id: key.hour_day_id,
            source_type_id: key.source_type_id,
            fuel_type_id: key.fuel_type_id,
            model_year_id: key.model_year_id,
            pol_process_id: key.pol_process_id,
            fully_weighted_mean_base_rate: rate,
            age_id: key.age_id,
        })
        .collect()
}

/// `BRPMC Step 3` — multiply emission rates by activity
/// (`SourceHoursOperatingActivity` section path).
///
/// Joins the `BRPMC Step 2` rate to `SHO` on `(hourDay, year, age,
/// sourceType)` and multiplies the rate by source-hours operating. The SQL's
/// `INSERT IGNORE` writes every joined row; there is no aggregation.
fn step3_unadjusted(
    inputs: &BasicRunningPmInputs,
    fully_weighted: &[FullyWeightedRate],
) -> Vec<UnadjustedEmission> {
    let mut out = Vec::new();
    for f in fully_weighted {
        for sho in &inputs.sho {
            if sho.hour_day_id != f.hour_day_id
                || sho.year_id != f.year_id
                || sho.age_id != f.age_id
                || sho.source_type_id != f.source_type_id
            {
                continue;
            }
            out.push(UnadjustedEmission {
                year_id: f.year_id,
                month_id: sho.month_id,
                hour_day_id: f.hour_day_id,
                source_type_id: f.source_type_id,
                fuel_type_id: f.fuel_type_id,
                model_year_id: f.model_year_id,
                pol_process_id: f.pol_process_id,
                unadjusted_emission_quant: f.fully_weighted_mean_base_rate * sho.sho,
            });
        }
    }
    out
}

/// `BRPMC Step 4-b`, first query — `FuelSupplyWithFuelType`.
///
/// Joins `FuelSupply` to `FuelFormulation`, `FuelSubType`, `MonthOfAnyYear`
/// and `Year` (the latter restricted to the run year) to pair each
/// formulation's market share with its fuel type.
fn fuel_supply_with_fuel_type(
    inputs: &BasicRunningPmInputs,
    year: i32,
) -> Vec<FuelSupplyWithFuelType> {
    let mut out = Vec::new();
    for fs in &inputs.fuel_supply {
        for ff in &inputs.fuel_formulation {
            if ff.fuel_formulation_id != fs.fuel_formulation_id {
                continue;
            }
            for fst in &inputs.fuel_sub_type {
                if fst.fuel_sub_type_id != ff.fuel_sub_type_id {
                    continue;
                }
                for may in &inputs.month_of_any_year {
                    if may.month_group_id != fs.month_group_id {
                        continue;
                    }
                    for y in &inputs.year {
                        if y.fuel_year_id != fs.fuel_year_id || y.year_id != year {
                            continue;
                        }
                        out.push(FuelSupplyWithFuelType {
                            year_id: y.year_id,
                            month_id: may.month_id,
                            fuel_formulation_id: fs.fuel_formulation_id,
                            fuel_type_id: fst.fuel_type_id,
                            market_share: fs.market_share,
                        });
                    }
                }
            }
        }
    }
    out
}

/// `BRPMC Step 4-b`, second query — `FuelSupplyAdjustment`.
///
/// The SQL cross-joins `County`, `PollutantProcessModelYear`,
/// `FuelSupplyWithFuelType` and `RunSpecSourceType`, left-joins
/// `generalFuelRatio` on the formulation / process / model-year / age /
/// source-type match, and sums the blended GPA / non-GPA fuel-effect ratio
/// weighted by market share, bucketed by `(year, month, polProcess,
/// modelYear, sourceType, fuelType)`. With no `generalFuelRatio` match the
/// `ifnull(…, 1)` coalesces give a blended ratio of `1`.
fn fuel_supply_adjustment(
    inputs: &BasicRunningPmInputs,
    year: i32,
    fuel_supply_with_fuel_type: &[FuelSupplyWithFuelType],
) -> Vec<FuelSupplyAdjustment> {
    /// Group key for the fuel-adjustment sum.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Key {
        year_id: i32,
        month_id: i32,
        pol_process_id: i32,
        model_year_id: i32,
        source_type_id: i32,
        fuel_type_id: i32,
    }

    let mut adjustment: BTreeMap<Key, f64> = BTreeMap::new();

    for county in &inputs.county {
        for ppmy in &inputs.pollutant_process_model_year {
            let age = year - ppmy.model_year_id;
            for fsft in fuel_supply_with_fuel_type {
                for rst in &inputs.run_spec_source_type {
                    // LEFT JOIN generalFuelRatio: collect every matching row;
                    // an empty match still contributes one term (ratios = 1).
                    let mut matched_any = false;
                    let mut accumulate = |gfr: Option<&GeneralFuelRatioRow>| {
                        let ratio = ifnull_one(gfr.and_then(|g| g.fuel_effect_ratio));
                        let ratio_gpa = ifnull_one(gfr.and_then(|g| g.fuel_effect_ratio_gpa));
                        let blended = ratio + county.gpa_fract * (ratio_gpa - ratio);
                        let key = Key {
                            year_id: fsft.year_id,
                            month_id: fsft.month_id,
                            pol_process_id: ppmy.pol_process_id,
                            model_year_id: ppmy.model_year_id,
                            source_type_id: rst.source_type_id,
                            fuel_type_id: fsft.fuel_type_id,
                        };
                        *adjustment.entry(key).or_insert(0.0) += blended * fsft.market_share;
                    };
                    for gfr in &inputs.general_fuel_ratio {
                        if gfr.fuel_formulation_id == fsft.fuel_formulation_id
                            && gfr.pol_process_id == ppmy.pol_process_id
                            && gfr.min_model_year_id <= ppmy.model_year_id
                            && gfr.max_model_year_id >= ppmy.model_year_id
                            && gfr.min_age_id <= age
                            && gfr.max_age_id >= age
                            && gfr.source_type_id == rst.source_type_id
                        {
                            matched_any = true;
                            accumulate(Some(gfr));
                        }
                    }
                    if !matched_any {
                        accumulate(None);
                    }
                }
            }
        }
    }

    adjustment
        .into_iter()
        .map(|(key, fuel_adjustment)| FuelSupplyAdjustment {
            year_id: key.year_id,
            month_id: key.month_id,
            pol_process_id: key.pol_process_id,
            model_year_id: key.model_year_id,
            source_type_id: key.source_type_id,
            fuel_type_id: key.fuel_type_id,
            fuel_adjustment,
        })
        .collect()
}

/// De-duplication key for the `BRPMC Step 4-c` `SELECT DISTINCT` — the seven
/// id columns of a `FuelAdjustedEmissionRate` row paired with the emission
/// quantity's `f64` bit pattern (`f64` is not `Hash`/`Eq`).
type FuelAdjustedDistinctKey = (i32, i32, i32, i32, i32, i32, i32, u64);

/// `BRPMC Step 4` — apply the fuel adjustment.
///
/// Builds `FuelSupplyWithFuelType` and `FuelSupplyAdjustment`
/// (sub-steps 4-b) and then joins `UnadjustedEmissionResults` to
/// `FuelSupplyAdjustment` on the six shared dimensions (4-c), scaling the
/// emission quantity by the fuel-adjustment factor. `SELECT DISTINCT`
/// collapses duplicate result rows.
fn step4_fuel_adjusted(
    inputs: &BasicRunningPmInputs,
    year: i32,
    unadjusted: &[UnadjustedEmission],
) -> Vec<FuelAdjustedEmission> {
    let fswft = fuel_supply_with_fuel_type(inputs, year);
    let adjustment = fuel_supply_adjustment(inputs, year, &fswft);

    let mut seen: HashSet<FuelAdjustedDistinctKey> = HashSet::new();
    let mut out = Vec::new();

    for u in unadjusted {
        for f in &adjustment {
            if f.year_id != u.year_id
                || f.month_id != u.month_id
                || f.source_type_id != u.source_type_id
                || f.fuel_type_id != u.fuel_type_id
                || f.model_year_id != u.model_year_id
                || f.pol_process_id != u.pol_process_id
            {
                continue;
            }
            // The SQL `coalesce(f.fuelAdjustment * q, q)` guards a NULL
            // `fuelAdjustment`. `fuelAdjustment` is a `SUM` over a non-empty
            // cross-join group, so it is never NULL here; the product is the
            // adjusted quantity.
            let emission_quant = f.fuel_adjustment * u.unadjusted_emission_quant;
            // `SELECT DISTINCT` over the seven id columns and the quantity.
            if seen.insert((
                u.year_id,
                u.month_id,
                u.hour_day_id,
                u.source_type_id,
                u.fuel_type_id,
                u.model_year_id,
                u.pol_process_id,
                emission_quant.to_bits(),
            )) {
                out.push(FuelAdjustedEmission {
                    year_id: u.year_id,
                    month_id: u.month_id,
                    hour_day_id: u.hour_day_id,
                    source_type_id: u.source_type_id,
                    fuel_type_id: u.fuel_type_id,
                    model_year_id: u.model_year_id,
                    pol_process_id: u.pol_process_id,
                    emission_quant,
                });
            }
        }
    }
    out
}

/// `BRPMC Step 5` — apply the temperature adjustment
/// (`ApplyTemperatureAdjustment` section path).
///
/// Joins the `BRPMC Step 4` quantity to `HourDay` and `ZoneMonthHour`, then
/// left-joins `TemperatureAdjustment` on `(polProcess, fuelType)` within the
/// model-year range. When a temperature-adjustment row matches and the
/// temperature is at or below 72 °F the quantity is scaled by
/// `exp(tempAdjustTermA · (72 − T))`; otherwise the quantity is unchanged.
fn step5_temperature_adjusted(
    inputs: &BasicRunningPmInputs,
    fuel_adjusted: &[FuelAdjustedEmission],
) -> Vec<AdjustedEmission> {
    let mut out = Vec::new();
    for u in fuel_adjusted {
        for hd in &inputs.hour_day {
            if hd.hour_day_id != u.hour_day_id {
                continue;
            }
            for zmh in &inputs.zone_month_hour {
                if zmh.month_id != u.month_id || zmh.hour_id != hd.hour_id {
                    continue;
                }
                // LEFT JOIN TemperatureAdjustment, model-year in range.
                let mut matched_any = false;
                let mut emit = |term_a: Option<f64>| {
                    let emission_quant =
                        temperature_adjusted_quant(u.emission_quant, zmh.temperature, term_a);
                    out.push(AdjustedEmission {
                        year_id: u.year_id,
                        month_id: u.month_id,
                        day_id: hd.day_id,
                        hour_id: hd.hour_id,
                        source_type_id: u.source_type_id,
                        fuel_type_id: u.fuel_type_id,
                        model_year_id: u.model_year_id,
                        pol_process_id: u.pol_process_id,
                        emission_quant,
                    });
                };
                for ta in &inputs.temperature_adjustment {
                    if ta.pol_process_id == u.pol_process_id
                        && ta.fuel_type_id == u.fuel_type_id
                        && ta.min_model_year_id <= u.model_year_id
                        && ta.max_model_year_id >= u.model_year_id
                    {
                        matched_any = true;
                        emit(ta.temp_adjust_term_a);
                    }
                }
                if !matched_any {
                    // No TemperatureAdjustment row: the SQL CASE falls to its
                    // ELSE 0, `exp(0) = 1`, quantity unchanged.
                    emit(None);
                }
            }
        }
    }
    out
}

/// The `BRPMC Step 5` `coalesce(q · exp(CASE …), q)` for one row.
///
/// `term_a` is the matched `TemperatureAdjustment.tempAdjustTermA`, or `None`
/// when no row matched (the SQL CASE then takes its `ELSE 0`). A matched but
/// SQL-`NULL` `tempAdjustTermA` makes the `exp(…)` `NULL`, from which the
/// `coalesce` falls back to the unadjusted quantity.
fn temperature_adjusted_quant(quant: f64, temperature: f64, term_a: Option<f64>) -> f64 {
    if temperature <= REFERENCE_TEMPERATURE_F {
        match term_a {
            Some(a) => quant * (a * (REFERENCE_TEMPERATURE_F - temperature)).exp(),
            // `exp(NULL)` is NULL; `coalesce` yields the unadjusted quantity.
            None => quant,
        }
    } else {
        // Above 72 °F the CASE is `ELSE 0`; `exp(0) = 1`.
        quant
    }
}

/// `BRPMC Step 6` — convert to the structure of `MOVESWorkerOutput`.
///
/// Joins `AdjustedEmissionResults` to `PollutantProcessAssoc` (splitting
/// `polProcessID` into `pollutantID` and `processID`) and cross-joins `Link`
/// (extract-filtered to the iteration's single link) for the road type. The
/// iteration geography is stamped on from the [`RunContext`].
fn step6_worker_output(
    inputs: &BasicRunningPmInputs,
    ctx: &RunContext,
    adjusted: &[AdjustedEmission],
) -> Vec<MovesWorkerOutputRow> {
    let mut out = Vec::new();
    for a in adjusted {
        for ppa in &inputs.pollutant_process_assoc {
            if ppa.pol_process_id != a.pol_process_id {
                continue;
            }
            for link in &inputs.link {
                out.push(MovesWorkerOutputRow {
                    year_id: a.year_id,
                    month_id: a.month_id,
                    day_id: a.day_id,
                    hour_id: a.hour_id,
                    state_id: ctx.state_id,
                    county_id: ctx.county_id,
                    zone_id: ctx.zone_id,
                    link_id: ctx.link_id,
                    pollutant_id: ppa.pollutant_id,
                    process_id: ppa.process_id,
                    source_type_id: a.source_type_id,
                    fuel_type_id: a.fuel_type_id,
                    model_year_id: a.model_year_id,
                    road_type_id: link.road_type_id,
                    emission_quant: a.emission_quant,
                });
            }
        }
    }
    out
}

/// The integer dimension tuple a worker-output row is sorted by, for
/// deterministic output. `MOVESWorkerOutput` is physically unordered in
/// MOVES; the order here is a presentation choice.
fn output_sort_key(row: &MovesWorkerOutputRow) -> (i32, i32, i32, i32, i32, i32, i32, i32, i32) {
    (
        row.year_id,
        row.month_id,
        row.day_id,
        row.hour_id,
        row.pollutant_id,
        row.process_id,
        row.source_type_id,
        row.fuel_type_id,
        row.model_year_id,
    )
}

/// The Basic Running PM Emission calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, as the
/// [`Calculator`] trait contract requires. All run-varying input flows
/// through [`BasicRunningPmEmissionCalculator::run`]'s arguments.
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicRunningPmEmissionCalculator;

impl BasicRunningPmEmissionCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Run the six-step `BasicPM25Calculator.sql` processing pipeline over a
    /// fully materialised set of input tables.
    ///
    /// Returns the emission rows in the structure of `MOVESWorkerOutput`,
    /// sorted by their integer dimension columns for deterministic output.
    #[must_use]
    pub fn run(
        &self,
        inputs: &BasicRunningPmInputs,
        ctx: &RunContext,
    ) -> Vec<MovesWorkerOutputRow> {
        let op_mode_weighted = step1_op_mode_weighted(inputs, ctx.year);
        let fully_weighted = step2_fully_weighted(inputs, ctx.year, &op_mode_weighted);
        let unadjusted = step3_unadjusted(inputs, &fully_weighted);
        let fuel_adjusted = step4_fuel_adjusted(inputs, ctx.year, &unadjusted);
        let adjusted = step5_temperature_adjusted(inputs, &fuel_adjusted);
        let mut output = step6_worker_output(inputs, ctx, &adjusted);
        output.sort_by_key(output_sort_key);
        output
    }
}

// ---------------------------------------------------------------------------
// TableRow wiring — data-plane round-trip for every input and output row type
// ---------------------------------------------------------------------------

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
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

impl TableRow for EmissionRateByAgeRow {
    fn table_name() -> &'static str {
        "EmissionRateByAge"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("ageGroupID".into(), DataType::Int32),
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
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
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
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let source_bin_id = get_i64("sourceBinID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let mean_base_rate = get_f64("meanBaseRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRateByAgeRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
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
            ("polProcessID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("sourceBinActivityFraction".into(), DataType::Float64),
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
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
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
        let pol_process_id = get_i32("polProcessID")?;
        let source_bin_id = get_i64("sourceBinID")?;
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let source_bin_activity_fraction = get_f64("sourceBinActivityFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinDistributionRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    source_type_model_year_id: source_type_model_year_id
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_bin_activity_fraction: source_bin_activity_fraction
                        .get(i)
                        .ok_or_else(|| null("sourceBinActivityFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AgeCategoryRow {
    fn table_name() -> &'static str {
        "AgeCategory"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("ageGroupID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
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
        let age_group_id = get_i32("ageGroupID")?;
        let age_id = get_i32("ageID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AgeCategoryRow {
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
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

impl TableRow for PollutantProcessModelYearRow {
    fn table_name() -> &'static str {
        "PollutantProcessModelYear"
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
        let t = "PollutantProcessModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_id = get_i32("modelYearID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessModelYearRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
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
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinRow {
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
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
        let hour_day_id = get_i32("hourDayID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let month_id = get_i32("monthID")?;
        let sho = get_f64("SHO")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
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
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
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
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        let month_group_id = get_i32("monthGroupID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                    month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                    market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
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
            ("fuelSubTypeID".into(), DataType::Int32),
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
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
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
        let fuel_sub_type_id = get_i32("fuelSubTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_sub_type_id: fuel_sub_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubTypeID"))?,
                })
            })
            .collect()
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
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
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
        let t = "FuelSubType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_sub_type_id = get_i32("fuelSubTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubTypeRow {
                    fuel_sub_type_id: fuel_sub_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
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

impl TableRow for RunSpecSourceTypeRow {
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
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecSourceTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for GeneralFuelRatioRow {
    fn table_name() -> &'static str {
        "generalFuelRatio"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("minAgeID".into(), DataType::Int32),
            ("maxAgeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelEffectRatio".into(), DataType::Float64),
            ("fuelEffectRatioGPA".into(), DataType::Float64),
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
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
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
                    "minAgeID".into(),
                    rows.iter().map(|r| r.min_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxAgeID".into(),
                    rows.iter().map(|r| r.max_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatio".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatioGPA".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio_gpa)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "generalFuelRatio";
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
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let min_age_id = get_i32("minAgeID")?;
        let max_age_id = get_i32("maxAgeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_effect_ratio = get_f64("fuelEffectRatio")?;
        let fuel_effect_ratio_gpa = get_f64("fuelEffectRatioGPA")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GeneralFuelRatioRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    min_model_year_id: min_model_year_id
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    min_age_id: min_age_id.get(i).ok_or_else(|| null("minAgeID"))?,
                    max_age_id: max_age_id.get(i).ok_or_else(|| null("maxAgeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_effect_ratio: fuel_effect_ratio.get(i),
                    fuel_effect_ratio_gpa: fuel_effect_ratio_gpa.get(i),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneMonthHourRow {
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    temperature: temperature.get(i).ok_or_else(|| null("temperature"))?,
                })
            })
            .collect()
    }
}

impl TableRow for TemperatureAdjustmentRow {
    fn table_name() -> &'static str {
        "TemperatureAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("tempAdjustTermA".into(), DataType::Float64),
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
                    "tempAdjustTermA".into(),
                    rows.iter()
                        .map(|r| r.temp_adjust_term_a)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "TemperatureAdjustment";
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
        let fuel_type_id = get_i32("fuelTypeID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let temp_adjust_term_a = get_f64("tempAdjustTermA")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(TemperatureAdjustmentRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    min_model_year_id: min_model_year_id
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    temp_adjust_term_a: temp_adjust_term_a.get(i),
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

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "Link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
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
        let link_id = get_i32("linkID")?;
        let road_type_id = get_i32("roadTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let state_id = get_i32("stateID")?;
        let county_id = get_i32("countyID")?;
        let zone_id = get_i32("zoneID")?;
        let link_id = get_i32("linkID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MovesWorkerOutputRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state_id.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                })
            })
            .collect()
    }
}

/// `BasicRunningPMEmissionCalculator` registers no `(pollutant, process)`
/// pairs — it is a legacy calculator superseded by `BaseRateCalculator`. See
/// [`Calculator::registrations`].
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-database tables the processing pipeline consumes — the names the
/// registry maps onto the per-run context.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "County",
    "EmissionRateByAge",
    "FuelFormulation",
    "FuelSubType",
    "FuelSupply",
    "generalFuelRatio",
    "HourDay",
    "Link",
    "MonthOfAnyYear",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessModelYear",
    "RunSpecSourceType",
    "SHO",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
    "TemperatureAdjustment",
    "Year",
    "ZoneMonthHour",
];

/// The calculator's single master-loop subscription — Running Exhaust at
/// `YEAR` granularity, `EMISSION_CALCULATOR` priority.
///
/// The Java `GenericCalculatorBase` constructor subscribes the calculator at
/// `MasterLoopGranularity.YEAR` and `MasterLoopPriority.EMISSION_CALCULATOR`
/// (priority offset 0) for each process in its `polProcessID` array; both
/// `11801` and `11201` decode to process 1, so there is one subscription.
/// `calculator-dag.json` records the granularity and priority but a
/// placeholder `process_id` of `0` — its static analyser could not resolve
/// the `polProcessID` strings; this resolves them.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        vec![CalculatorSubscription::new(
            ProcessId(RUNNING_EXHAUST_PROCESS_ID),
            Granularity::Year,
            priority,
        )]
    })
}

impl Calculator for BasicRunningPmEmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    /// Empty — `BasicRunningPMEmissionCalculator` is a legacy calculator
    /// superseded by `BaseRateCalculator` (migration-plan Task 45).
    ///
    /// The Java `GenericCalculatorBase` constructor's `polProcessID` array
    /// `{ "11801", "11201" }` would register the calculator for elemental
    /// carbon PM2.5 (112) and composite non-EC PM2.5 (118), both for Running
    /// Exhaust (process 1). But `BaseRateCalculator` already registers those
    /// two pairs — pollutants 112 and 118 are in its exhaust-pollutant set,
    /// process 1 in its process set — so a current MOVES run produces this
    /// output through the base-rate path, and `calculator-dag.json` records
    /// `registrations_count: 0` here. Returning the pairs would double-register
    /// them against `BaseRateCalculator`; this method returns the empty slice
    /// the trait permits.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let run_ctx = RunContext {
            year: pos.time.year.map(|y| i32::from(y)).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
        };
        let inputs = BasicRunningPmInputs {
            op_mode_distribution: tables.iter_typed("OpModeDistribution")?,
            emission_rate_by_age: tables.iter_typed("EmissionRateByAge")?,
            source_bin_distribution: tables.iter_typed("SourceBinDistribution")?,
            age_category: tables.iter_typed("AgeCategory")?,
            source_type_model_year: tables.iter_typed("SourceTypeModelYear")?,
            pollutant_process_model_year: tables.iter_typed("PollutantProcessModelYear")?,
            source_bin: tables.iter_typed("SourceBin")?,
            sho: tables.iter_typed("SHO")?,
            fuel_supply: tables.iter_typed("FuelSupply")?,
            fuel_formulation: tables.iter_typed("FuelFormulation")?,
            fuel_sub_type: tables.iter_typed("FuelSubType")?,
            month_of_any_year: tables.iter_typed("MonthOfAnyYear")?,
            year: tables.iter_typed("Year")?,
            county: tables.iter_typed("County")?,
            run_spec_source_type: tables.iter_typed("RunSpecSourceType")?,
            general_fuel_ratio: tables.iter_typed("generalFuelRatio")?,
            hour_day: tables.iter_typed("HourDay")?,
            zone_month_hour: tables.iter_typed("ZoneMonthHour")?,
            temperature_adjustment: tables.iter_typed("TemperatureAdjustment")?,
            pollutant_process_assoc: tables.iter_typed("PollutantProcessAssoc")?,
            link: tables.iter_typed("Link")?,
        };
        let rows = self.run(&inputs, &run_ctx);
        crate::wiring::emit_rows(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the minimal input set that carries one emission through all six
    /// steps. One operating mode, one source bin, one age, one fuel type, one
    /// month/hour — so each step's join resolves to a single row and the
    /// arithmetic can be checked by hand.
    fn single_path_inputs() -> (BasicRunningPmInputs, RunContext) {
        let ctx = RunContext {
            year: 2020,
            state_id: 26,
            county_id: 26161,
            zone_id: 261_610,
            link_id: 2_616_101,
        };
        let inputs = BasicRunningPmInputs {
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 205,
                pol_process_id: 11201,
                op_mode_id: 300,
                op_mode_fraction: 0.5,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                pol_process_id: 11201,
                op_mode_id: 300,
                source_bin_id: 900,
                age_group_id: 3,
                mean_base_rate: 4.0,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                pol_process_id: 11201,
                source_bin_id: 900,
                source_type_model_year_id: 7000,
                source_bin_activity_fraction: 0.5,
            }],
            age_category: vec![AgeCategoryRow {
                age_group_id: 3,
                age_id: 5,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 7000,
                source_type_id: 21,
                model_year_id: 2015, // year - age = 2020 - 5
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 11201,
                model_year_id: 2015,
                model_year_group_id: 0,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 900,
                fuel_type_id: 2,
                model_year_group_id: 0,
            }],
            sho: vec![ShoRow {
                hour_day_id: 205,
                year_id: 2020,
                age_id: 5,
                source_type_id: 21,
                month_id: 7,
                sho: 10.0,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_formulation_id: 4000,
                fuel_year_id: 2020,
                month_group_id: 7,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 4000,
                fuel_sub_type_id: 20,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 20,
                fuel_type_id: 2,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 7,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            county: vec![CountyRow {
                county_id: 26161,
                gpa_fract: 0.0,
            }],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            general_fuel_ratio: Vec::new(),
            hour_day: vec![HourDayRow {
                hour_day_id: 205,
                day_id: 5,
                hour_id: 20,
            }],
            zone_month_hour: vec![ZoneMonthHourRow {
                month_id: 7,
                hour_id: 20,
                temperature: 80.0, // above 72 °F → no temperature scaling
            }],
            temperature_adjustment: Vec::new(),
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 11201,
                pollutant_id: 112,
                process_id: 1,
            }],
            link: vec![LinkRow {
                link_id: 2_616_101,
                road_type_id: 4,
            }],
        };
        (inputs, ctx)
    }

    #[test]
    fn calculator_metadata() {
        let calc = BasicRunningPmEmissionCalculator::new();
        assert_eq!(calc.name(), "BasicRunningPMEmissionCalculator");
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"OpModeDistribution"));
        assert!(calc.input_tables().contains(&"TemperatureAdjustment"));
    }

    #[test]
    fn subscription_is_running_exhaust_year_emission_calculator() {
        let calc = BasicRunningPmEmissionCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        // Both Java `polProcessID` strings decode to process 1.
        assert_eq!(subs[0].process_id, ProcessId(1));
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");
    }

    #[test]
    fn registrations_are_empty_superseded_by_base_rate_calculator() {
        // A legacy calculator superseded by BaseRateCalculator: its (112, 1)
        // and (118, 1) pairs are registered to BaseRateCalculator, so this
        // calculator registers nothing to avoid double-registration.
        let calc = BasicRunningPmEmissionCalculator::new();
        assert!(calc.registrations().is_empty());
    }

    #[test]
    fn calculator_is_object_safe() {
        let calcs: Vec<Box<dyn Calculator>> =
            vec![Box::new(BasicRunningPmEmissionCalculator::new())];
        assert_eq!(calcs[0].name(), "BasicRunningPMEmissionCalculator");
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};

        let (inputs, _run_ctx) = single_path_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "OpModeDistribution",
            OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution.clone()).unwrap(),
        );
        store.insert(
            "EmissionRateByAge",
            EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age.clone()).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution.clone())
                .unwrap(),
        );
        store.insert(
            "AgeCategory",
            AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap(),
        );
        store.insert(
            "PollutantProcessModelYear",
            PollutantProcessModelYearRow::into_dataframe(
                inputs.pollutant_process_model_year.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin.clone()).unwrap(),
        );
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho.clone()).unwrap());
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply.clone()).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation.clone()).unwrap(),
        );
        store.insert(
            "FuelSubType",
            FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type.clone()).unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            MonthOfAnyYearRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap(),
        );
        store.insert(
            "Year",
            YearRow::into_dataframe(inputs.year.clone()).unwrap(),
        );
        store.insert(
            "County",
            CountyRow::into_dataframe(inputs.county.clone()).unwrap(),
        );
        store.insert(
            "RunSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(inputs.run_spec_source_type.clone()).unwrap(),
        );
        store.insert(
            "generalFuelRatio",
            GeneralFuelRatioRow::into_dataframe(inputs.general_fuel_ratio.clone()).unwrap(),
        );
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day.clone()).unwrap(),
        );
        store.insert(
            "ZoneMonthHour",
            ZoneMonthHourRow::into_dataframe(inputs.zone_month_hour.clone()).unwrap(),
        );
        store.insert(
            "TemperatureAdjustment",
            TemperatureAdjustmentRow::into_dataframe(inputs.temperature_adjustment.clone())
                .unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc.clone())
                .unwrap(),
        );
        store.insert(
            "Link",
            LinkRow::into_dataframe(inputs.link.clone()).unwrap(),
        );
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 261_610, 2_616_101),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = BasicRunningPmEmissionCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(
            out.dataframe().unwrap().height() > 0,
            "expected at least one row"
        );
    }

    #[test]
    fn run_on_empty_inputs_yields_no_rows() {
        let calc = BasicRunningPmEmissionCalculator::new();
        let out = calc.run(&BasicRunningPmInputs::default(), &RunContext::default());
        assert!(out.is_empty());
    }

    #[test]
    fn single_path_carries_one_emission_through_all_six_steps() {
        // Step 1: opModeFraction(0.5) * meanBaseRate(4.0)        = 2.0
        // Step 2: sourceBinActivityFraction(0.5) * 2.0           = 1.0
        // Step 3: fullyWeightedMeanBaseRate(1.0) * SHO(10.0)     = 10.0
        // Step 4: no generalFuelRatio match, gpaFract 0 → ratio 1, *10.0 = 10.0
        // Step 5: temperature 80 °F > 72 °F → no scaling         = 10.0
        let calc = BasicRunningPmEmissionCalculator::new();
        let (inputs, ctx) = single_path_inputs();
        let out = calc.run(&inputs, &ctx);
        assert_eq!(out.len(), 1);
        let row = out[0];
        assert!((row.emission_quant - 10.0).abs() < 1e-12);
        assert_eq!(row.pollutant_id, 112);
        assert_eq!(row.process_id, 1);
        assert_eq!(row.fuel_type_id, 2);
        assert_eq!(row.model_year_id, 2015);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 20);
        assert_eq!(row.road_type_id, 4);
        // Geography is stamped from the RunContext.
        assert_eq!(row.state_id, 26);
        assert_eq!(row.county_id, 26161);
        assert_eq!(row.zone_id, 261_610);
        assert_eq!(row.link_id, 2_616_101);
    }

    #[test]
    fn temperature_below_reference_scales_the_quantity_up() {
        // temperature 62 °F ≤ 72 °F, with a TemperatureAdjustment row:
        // factor = exp(tempAdjustTermA * (72 - 62)) = exp(0.01 * 10).
        let calc = BasicRunningPmEmissionCalculator::new();
        let (mut inputs, ctx) = single_path_inputs();
        inputs.zone_month_hour[0].temperature = 62.0;
        inputs.temperature_adjustment = vec![TemperatureAdjustmentRow {
            pol_process_id: 11201,
            fuel_type_id: 2,
            min_model_year_id: 1990,
            max_model_year_id: 2050,
            temp_adjust_term_a: Some(0.01),
        }];
        let out = calc.run(&inputs, &ctx);
        assert_eq!(out.len(), 1);
        let expected = 10.0 * (0.01_f64 * 10.0).exp();
        assert!((out[0].emission_quant - expected).abs() < 1e-9);
    }

    #[test]
    fn temperature_adjustment_outside_model_year_range_does_not_apply() {
        // The model year (2015) falls outside the TemperatureAdjustment
        // range, so the left join misses and the quantity is unchanged.
        let calc = BasicRunningPmEmissionCalculator::new();
        let (mut inputs, ctx) = single_path_inputs();
        inputs.zone_month_hour[0].temperature = 62.0;
        inputs.temperature_adjustment = vec![TemperatureAdjustmentRow {
            pol_process_id: 11201,
            fuel_type_id: 2,
            min_model_year_id: 1990,
            max_model_year_id: 2000, // 2015 is out of range
            temp_adjust_term_a: Some(0.01),
        }];
        let out = calc.run(&inputs, &ctx);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 10.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_adjustment_blends_gpa_and_non_gpa_ratios() {
        // gpaFract 0.25, fuelEffectRatio 2.0, fuelEffectRatioGPA 6.0:
        // blended = 2.0 + 0.25 * (6.0 - 2.0) = 3.0; emission 10.0 → 30.0.
        let calc = BasicRunningPmEmissionCalculator::new();
        let (mut inputs, ctx) = single_path_inputs();
        inputs.county[0].gpa_fract = 0.25;
        inputs.general_fuel_ratio = vec![GeneralFuelRatioRow {
            fuel_formulation_id: 4000,
            pol_process_id: 11201,
            min_model_year_id: 1990,
            max_model_year_id: 2050,
            min_age_id: 0,
            max_age_id: 40,
            source_type_id: 21,
            fuel_effect_ratio: Some(2.0),
            fuel_effect_ratio_gpa: Some(6.0),
        }];
        let out = calc.run(&inputs, &ctx);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 30.0).abs() < 1e-9);
    }

    #[test]
    fn op_mode_weighting_sums_over_operating_modes() {
        // Two operating modes contribute to the same source bin / age; their
        // opModeFraction * meanBaseRate products sum in BRPMC Step 1.
        // mode 300: 0.5 * 4.0 = 2.0;  mode 301: 0.5 * 8.0 = 4.0;  sum = 6.0.
        // Step 2: 0.5 * 6.0 = 3.0;  Step 3: 3.0 * 10.0 = 30.0.
        let calc = BasicRunningPmEmissionCalculator::new();
        let (mut inputs, ctx) = single_path_inputs();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            source_type_id: 21,
            hour_day_id: 205,
            pol_process_id: 11201,
            op_mode_id: 301,
            op_mode_fraction: 0.5,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            pol_process_id: 11201,
            op_mode_id: 301,
            source_bin_id: 900,
            age_group_id: 3,
            mean_base_rate: 8.0,
        });
        let out = calc.run(&inputs, &ctx);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 30.0).abs() < 1e-9);
    }

    #[test]
    fn step1_distinct_collapses_source_bin_join_multiplicity() {
        // One (operating mode, rate) pair reaches the BRPMC Step 1 output
        // through two SourceBinDistribution → SourceTypeModelYear chains —
        // two source-type-model-year surrogates that resolve to the same
        // model year. Both chains emit the identical six-column row; the
        // SourceBinDistribution / SourceTypeModelYear columns are not among
        // the six, so `SELECT DISTINCT` collapses the pair and the rate is
        // summed exactly once.
        let inputs = BasicRunningPmInputs {
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 205,
                pol_process_id: 11201,
                op_mode_id: 300,
                op_mode_fraction: 0.5,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                pol_process_id: 11201,
                op_mode_id: 300,
                source_bin_id: 900,
                age_group_id: 3,
                mean_base_rate: 4.0,
            }],
            source_bin_distribution: vec![
                SourceBinDistributionRow {
                    pol_process_id: 11201,
                    source_bin_id: 900,
                    source_type_model_year_id: 7000,
                    source_bin_activity_fraction: 0.5,
                },
                SourceBinDistributionRow {
                    pol_process_id: 11201,
                    source_bin_id: 900,
                    source_type_model_year_id: 7001,
                    source_bin_activity_fraction: 0.5,
                },
            ],
            age_category: vec![AgeCategoryRow {
                age_group_id: 3,
                age_id: 5,
            }],
            source_type_model_year: vec![
                SourceTypeModelYearRow {
                    source_type_model_year_id: 7000,
                    source_type_id: 21,
                    model_year_id: 2015,
                },
                SourceTypeModelYearRow {
                    source_type_model_year_id: 7001,
                    source_type_id: 21,
                    model_year_id: 2015,
                },
            ],
            ..BasicRunningPmInputs::default()
        };
        let weighted = step1_op_mode_weighted(&inputs, 2020);
        assert_eq!(weighted.len(), 1);
        // opModeFraction(0.5) * meanBaseRate(4.0) = 2.0, counted once.
        assert!((weighted[0].op_mode_weighted_mean_base_rate - 2.0).abs() < 1e-12);
    }

    #[test]
    fn temperature_adjusted_quant_handles_the_three_cases() {
        // Above the reference temperature: identity.
        assert!((temperature_adjusted_quant(10.0, 80.0, Some(0.01)) - 10.0).abs() < 1e-12);
        // At or below, with a coefficient: exponential scaling.
        let expected = 10.0 * (0.01_f64 * (72.0 - 60.0)).exp();
        assert!((temperature_adjusted_quant(10.0, 60.0, Some(0.01)) - expected).abs() < 1e-9);
        // At or below, but a NULL coefficient: `exp(NULL)` → coalesce → q.
        assert!((temperature_adjusted_quant(10.0, 60.0, None) - 10.0).abs() < 1e-12);
    }

    #[test]
    fn no_running_exhaust_activity_yields_no_rows() {
        // Drop the SHO activity: BRPMC Step 3 produces nothing, so the
        // pipeline yields no rows.
        let calc = BasicRunningPmEmissionCalculator::new();
        let (mut inputs, ctx) = single_path_inputs();
        inputs.sho.clear();
        let out = calc.run(&inputs, &ctx);
        assert!(out.is_empty());
    }
}
