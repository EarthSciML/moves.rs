//! Port of `database/BasicPM25Calculator.sql` — the
//! `BasicBrakeWearPMEmissionCalculator` and `BasicTireWearPMEmissionCalculator`
//! pair, MOVES's brake-wear and tire-wear PM2.5 calculators.
//!
//! Migration plan: Phase 3, Task 56.
//!
//! # What these calculators do
//!
//! MOVES tracks the particulate matter abraded from brake pads and tires as
//! two ordinary emission processes with their own PM2.5 pollutants:
//!
//! | Java class | Pollutant | Process | Java `polProcessID` |
//! |------------|-----------|---------|---------------------|
//! | `BasicBrakeWearPMEmissionCalculator` | 116 — Primary PM2.5 Brakewear Particulate | 9 — Brakewear | `"11609"` |
//! | `BasicTireWearPMEmissionCalculator`  | 117 — Primary PM2.5 Tirewear Particulate  | 10 — Tirewear | `"11710"` |
//!
//! Each `polProcessID` encodes `pollutantID * 100 + processID`.
//!
//! # One algorithm, two thin subclasses
//!
//! Both Java classes are minimal `GenericCalculatorBase` subclasses whose
//! constructors differ only in the single `polProcessID` they declare. They
//! name the **same** SQL script — `database/BasicPM25Calculator.sql` — with
//! the **same** four enabled sections: `HasManyOpModes`, `EmissionRateRates`,
//! `SourceHoursOperatingActivity`, `NoTemperatureAdjustment`. So their compute
//! core is byte-for-byte identical; only the `polProcessID` filtering the
//! extracted tables differs. This module ports that shared core once, as
//! [`BasicPm25Calculator::run`], and gives each Java class a thin
//! [`Calculator`] implementor — [`BasicBrakeWearPmEmissionCalculator`] and
//! [`BasicTireWearPmEmissionCalculator`] — that carries only the differing
//! subscription metadata and delegates its numerics to the shared `run`.
//!
//! # Supersession by `BaseRateCalculator`
//!
//! Both are **legacy** calculators that the modern rates-first
//! `BaseRateCalculator` (Task 45) superseded. The pinned MOVES runtime
//! registry `CalculatorInfo.txt` carries no `BasicBrakeWearPMEmissionCalculator`
//! or `BasicTireWearPMEmissionCalculator` entry — the brake-wear `(116, 9)`
//! and tire-wear `(117, 10)` pairs are registered to `BaseRateCalculator`
//! instead — and `characterization/calculator-chains/calculator-dag.json`
//! records `registrations_count: 0` for both. [`Calculator::registrations`]
//! therefore returns an **empty slice** for each: re-registering the pairs
//! here would collide with the already-merged `BaseRateCalculator`. The
//! compute core is still ported in full (it is the task's real deliverable),
//! and [`Calculator::subscriptions`] still mirrors the Java `subscribeToMe`.
//!
//! # Java / SQL structure
//!
//! Unlike the rates-path calculators (e.g. `BaseRateCalculator`), these are
//! legacy inventory-path calculators with **no Go worker**: each Java class is
//! a thin `GenericCalculatorBase` subclass that names a SQL script, and the
//! script `database/BasicPM25Calculator.sql` carries the whole computation.
//! This module ports that script's `Section Processing`; the script's
//! `Section Extract Data` is the data-plane table load that Task 50
//! (`DataFrameStore`) materialises.
//!
//! The script carries paired section toggles for variants this calculator
//! pair does not use. Per the four enabled sections above, this port
//! implements the `EmissionRateRates` / `HasManyOpModes` rate path, the
//! `SourceHoursOperatingActivity` activity path, and the
//! `NoTemperatureAdjustment` (pass-through) temperature path **only**; the
//! `EmissionRateByAgeRates`, `HasOneOpMode`, `SourceHoursActivity`,
//! `StartsActivity`, `ApplyTemperatureAdjustment` and
//! `ApplyLinearTemperatureAdjustment` blocks are dead for brake/tire wear and
//! are documented as reference-only.
//!
//! # The six-step pipeline
//!
//! 1. **Weight emission rates by operating mode** (`weight_by_op_mode`) —
//!    `opModeFraction * meanBaseRate`, joined `OpModeDistribution` ⋈
//!    `EmissionRate` and gated by the
//!    `SourceBinDistribution`/`AgeCategory`/`SourceTypeModelYear` existence
//!    chain. Produces the `OpModeWeightedEmissionRate` rows.
//! 2. **Weight by source bin** (`weight_by_source_bin`) — sums
//!    `sourceBinActivityFraction * opModeWeightedMeanBaseRate` across source
//!    bins, resolving the fuel type through `SourceBin`. Produces the
//!    `FullyWeightedEmissionRate` rows.
//! 3. **Multiply by activity** (`multiply_by_activity`) — multiplies the
//!    fully-weighted rate by the `SHO` (source-hours-operating) count.
//!    Produces the `UnadjustedEmissionResults` rows.
//! 4. **Apply fuel adjustment** (`apply_fuel_adjustment`, fed by
//!    `build_fuel_supply_with_fuel_type` and `build_fuel_supply_adjustment`) —
//!    builds the per-fuel-type `generalFuelRatio` market-share-weighted
//!    adjustment and scales each unadjusted quantity by it. Produces the
//!    `FuelAdjustedEmissionRate` rows.
//! 5. **Apply temperature adjustment** (`decode_hour_day`) — for these
//!    calculators the enabled `NoTemperatureAdjustment` section copies the
//!    quantity through unchanged, decoding `hourDayID` into `(dayID, hourID)`
//!    via `HourDay`. Produces the `AdjustedEmissionResults` rows.
//! 6. **Convert to worker output** (`to_worker_output`) — splits
//!    `polProcessID` into `(pollutant, process)` through `PollutantProcessAssoc`
//!    and attaches the constant location columns. Produces [`WorkerOutputRow`]s.
//!
//! [`BasicPm25Calculator::run`] chains all six.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` intermediate columns.** The SQL holds every intermediate rate
//!   (`opModeWeightedMeanBaseRate`, `fullyWeightedMeanBaseRate`,
//!   `unadjustedEmissionQuant`, `fuelAdjustment`, `emissionQuant`) in 32-bit
//!   `FLOAT` temp columns, and the `meanBaseRate` / `opModeFraction` /
//!   `sourceBinActivityFraction` / `SHO` / `marketShare` source columns are
//!   `FLOAT` too. MariaDB evaluates the arithmetic in `DOUBLE` but truncates
//!   to `f32` at each temp-table write. This port computes in `f64`
//!   throughout; per the Task 33 / Task 41 / Task 54 precedent the
//!   bug-compatibility decision is deferred to Task 44.
//! * **`DISTINCT`-then-`SUM` in step 1.** The `OpModeWeightedEmissionRateTemp`
//!   insert is a `SELECT DISTINCT` that does **not** project `opModeID`, and
//!   the `OpModeWeightedEmissionRate` insert that follows sums the temp table
//!   grouped by the five non-rate columns. Two operating modes that yield a
//!   bit-identical `opModeFraction * meanBaseRate` for the same five-column
//!   key therefore collapse to a single `DISTINCT` row and are summed **once**,
//!   not twice. This port reproduces that: the temp table is modeled as a set
//!   of `(key, rate)` pairs (`weight_by_op_mode`). MariaDB compares the
//!   `f32`-truncated rate; this port compares the `f64` rate — flagged for
//!   Task 44 alongside the `FLOAT`-column note above.
//! * **`meanBaseRate` is `FLOAT NULL`.** A `NULL` `EmissionRate.meanBaseRate`
//!   would propagate a SQL `NULL` through the step-1 product. Matching the
//!   Task 54 precedent, this port models `meanBaseRate` as a present `f64`;
//!   the data plane (Task 50) decides how to surface a `NULL`.
//! * **No division.** The processing pipeline contains no integer division,
//!   so the MariaDB `int / int` rounding gotcha does not arise.
//!
//! # No road-type gate
//!
//! `GenericCalculatorBase.doExecute` gates only the Start Exhaust and Extended
//! Idle Exhaust processes to the off-network road type; Brakewear and Tirewear
//! are ungated and run on every road type. Neither Java class implements
//! `MasterLoopContext.IContextFilter`, so — unlike `BasicStartPMEmissionCalculator`
//! (Task 54) — this module needs no `processes_road_type` gate.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders, so it
//! cannot yet materialise a [`BasicPm25Inputs`] nor write the worker output
//! back. The numerically faithful pipeline is fully ported and unit-tested on
//! [`BasicPm25Calculator::run`]; once the `DataFrameStore` lands, `execute`
//! builds the inputs from `ctx.tables()`, calls `run`, and stores the
//! [`WorkerOutputRow`]s.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// MOVES process id for Brakewear — the process
/// `BasicBrakeWearPMEmissionCalculator` runs in (Java `polProcessID` `"11609"`).
const BRAKEWEAR_PROCESS_ID: u16 = 9;
/// MOVES process id for Tirewear — the process
/// `BasicTireWearPMEmissionCalculator` runs in (Java `polProcessID` `"11710"`).
const TIREWEAR_PROCESS_ID: u16 = 10;

// ===========================================================================
// Input row structs — one per default-DB table the processing section reads.
// ===========================================================================

/// One `OpModeDistribution` row — the operating-mode fraction of activity.
///
/// MOVES keys this table by `(sourceTypeID, hourDayID, linkID, polProcessID,
/// opModeID)`; the extract filters to the iteration link, so `linkID` is
/// constant and not modeled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `hourDayID` — joined hour-of-day / day-of-week bucket.
    pub hour_day_id: i32,
    /// `sourceTypeID` — vehicle source type.
    pub source_type_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — fraction of activity in this operating mode.
    pub op_mode_fraction: f64,
}

/// One `EmissionRate` row — the per-operating-mode mean base rate.
///
/// `EmissionRate` carries no age dimension (unlike `EmissionRateByAge`); the
/// age group is supplied by the cross-joined `AgeCategory` in step 1.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateRow {
    /// `sourceBinID` — source bin (a `BIGINT` in MOVES).
    pub source_bin_id: i64,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `meanBaseRate` — the mean base emission rate (`FLOAT NULL` in MOVES;
    /// see the module-level fidelity note).
    pub mean_base_rate: f64,
}

/// One `SourceBinDistribution` row — the source-bin activity split.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID` — the source-type / model-year key.
    pub source_type_model_year_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — fraction of activity in this bin.
    pub source_bin_activity_fraction: f64,
}

/// One `AgeCategory` row — maps a single vehicle age to its age group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageID` — the vehicle age in years.
    pub age_id: i32,
    /// `ageGroupID` — the age group `ageID` falls in.
    pub age_group_id: i32,
}

/// One `SourceTypeModelYear` row — ties a `sourceTypeModelYearID` to its
/// `(sourceTypeID, modelYearID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `modelYearID` — the vehicle model year.
    pub model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `PollutantProcessModelYear` row — maps `(polProcessID, modelYearID)`
/// to a model-year group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
}

/// One `SourceBin` row — the fuel type and model-year group of a source bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `fuelTypeID` — the fuel type of the bin.
    pub fuel_type_id: i32,
    /// `modelYearGroupID` — the bin's model-year group.
    pub model_year_group_id: i32,
}

/// One `SHO` row — the source-hours-operating activity count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `SHO` — the source-hours-operating count.
    pub sho: f64,
}

/// One `HourDay` row — decodes an `hourDayID` into day-of-week and hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `dayID` — day-of-week bucket.
    pub day_id: i32,
    /// `hourID` — hour-of-day.
    pub hour_id: i32,
}

/// One `FuelSupply` row — a fuel formulation's market share in a month group.
///
/// The extract filters `FuelSupply` to the iteration fuel region, so
/// `fuelRegionID` is constant and not modeled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `monthGroupID` — the month group the share applies to.
    pub month_group_id: i32,
    /// `fuelYearID` — the fuel year.
    pub fuel_year_id: i32,
    /// `marketShare` — fraction of the fuel market this formulation holds.
    pub market_share: f64,
}

/// One `FuelFormulation` row — ties a formulation to its fuel subtype.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_sub_type_id: i32,
}

/// One `FuelSubType` row — ties a fuel subtype to its fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubTypeRow {
    /// `fuelSubtypeID`.
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

/// One `Year` row — ties a calendar year to its fuel year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — the calendar year.
    pub year_id: i32,
    /// `fuelYearID` — the fuel year `yearID` maps to.
    pub fuel_year_id: i32,
}

/// One `RunSpecSourceType` row — a source type the RunSpec selected.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `generalFuelRatio` row — a fuel-effect multiplier and its applicability
/// window.
///
/// `fuelEffectRatio` / `fuelEffectRatioGPA` are nullable in MOVES; the SQL
/// wraps both in `ifnull(..., 1)`, so a `None` here ports as a multiplier of
/// `1.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeneralFuelRatioRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `minModelYearID` — inclusive lower bound of the applicable model years.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the applicable model years.
    pub max_model_year_id: i32,
    /// `minAgeID` — inclusive lower bound of the applicable vehicle ages.
    pub min_age_id: i32,
    /// `maxAgeID` — inclusive upper bound of the applicable vehicle ages.
    pub max_age_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelEffectRatio` — the non-GPA fuel-effect multiplier, or `None` for
    /// SQL `NULL` (`ifnull` defaults it to `1.0`).
    pub fuel_effect_ratio: Option<f64>,
    /// `fuelEffectRatioGPA` — the geographic-phase-in-area fuel-effect
    /// multiplier, or `None` for SQL `NULL` (`ifnull` defaults it to `1.0`).
    pub fuel_effect_ratio_gpa: Option<f64>,
}

/// One `PollutantProcessAssoc` row — splits a `polProcessID` into its
/// `(pollutantID, processID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
}

/// The fully materialised set of input tables one calculator run consumes.
///
/// Mirrors the tables `BasicPM25Calculator.sql`'s `Section Processing` reads.
/// The data plane (Task 50) builds this from the filtered execution database;
/// tests build it directly.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicPm25Inputs {
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `EmissionRate` rows.
    pub emission_rate: Vec<EmissionRateRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `PollutantProcessModelYear` rows.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SHO` rows.
    pub sho: Vec<ShoRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `FuelSupply` rows.
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubType` rows.
    pub fuel_sub_type: Vec<FuelSubTypeRow>,
    /// `MonthOfAnyYear` rows.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `Year` rows.
    pub year: Vec<YearRow>,
    /// `RunSpecSourceType` rows.
    pub run_spec_source_type: Vec<RunSpecSourceTypeRow>,
    /// `generalFuelRatio` rows.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
}

/// The scalar run constants the SQL reads from `##context...##` macros and
/// from the single-row `County` / `Link` extracts.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RunConstants {
    /// `##context.year##` — the calendar year of the run.
    pub year: i32,
    /// `##context.iterLocation.stateRecordID##`.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##`.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##`.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##`.
    pub link_id: i32,
    /// `roadTypeID` of the iteration link (the cross-joined `Link.roadTypeID`).
    pub road_type_id: i32,
    /// `County.GPAFract` — the geographic-phase-in-area fraction; the single
    /// `County` extract row makes it a run constant. Modeled as a present
    /// `f64`: a `NULL` would null the step-4 fuel-adjustment term, a case the
    /// data plane (Task 50) resolves.
    pub gpa_fract: f64,
}

// ===========================================================================
// `f64` total order — for the SQL `DISTINCT` ports.
// ===========================================================================

/// A wrapper giving `f64` a total order, so a rate can key a `BTreeSet`.
///
/// MOVES `SELECT DISTINCT` and the `FLOAT` temp tables behind it deduplicate
/// on the *value* of a rate column. Rust's `f64` is only `PartialOrd`, so the
/// `DISTINCT` ports (`weight_by_op_mode`, `apply_fuel_adjustment`) need a
/// total order; [`f64::total_cmp`] supplies one. The emission rates and
/// quantities flowing through here are finite and non-negative, so
/// `total_cmp` agrees with the numeric order and with SQL equality — the lone
/// exception, `-0.0` sorting below `+0.0`, cannot arise from a product of
/// non-negative operands.
#[derive(Debug, Clone, Copy)]
struct OrderedRate(f64);

impl PartialEq for OrderedRate {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for OrderedRate {}

impl PartialOrd for OrderedRate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedRate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

// ===========================================================================
// Intermediate result rows — one per SQL temp table in `Section Processing`.
// ===========================================================================

/// Step 1 output — `OpModeWeightedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OpModeWeightedRate {
    hour_day_id: i32,
    source_type_id: i32,
    source_bin_id: i64,
    age_group_id: i32,
    pol_process_id: i32,
    op_mode_weighted_mean_base_rate: f64,
}

/// Step 2 output — `FullyWeightedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
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

/// Step 3 output — `UnadjustedEmissionResults`.
#[derive(Debug, Clone, Copy, PartialEq)]
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

/// Step 4-b intermediate — `FuelSupplyWithFuelType`.
///
/// The SQL projects `countyID` too, but it is the single-row `County`
/// extract's constant and no downstream join uses it, so it is dropped here.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelSupplyWithFuelType {
    year_id: i32,
    month_id: i32,
    fuel_formulation_id: i32,
    fuel_type_id: i32,
    market_share: f64,
}

/// Step 4-c output — `FuelAdjustedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelAdjustedRate {
    year_id: i32,
    month_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    emission_quant: f64,
}

/// Step 5 output — `AdjustedEmissionResults`.
#[derive(Debug, Clone, Copy, PartialEq)]
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

/// One `MOVESWorkerOutput` row — the calculator's contribution to the master
/// emission tally.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerOutputRow {
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
    /// `emissionQuant` — the emission quantity.
    pub emission_quant: f64,
}

// ===========================================================================
// Aggregation keys — named structs keep the multi-column `GROUP BY` keys
// readable (and dodge clippy's `type_complexity` on big tuple keys).
// ===========================================================================

/// `GROUP BY` key of step 1's `OpModeWeightedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OpModeWeightedKey {
    hour_day_id: i32,
    source_type_id: i32,
    source_bin_id: i64,
    age_group_id: i32,
    pol_process_id: i32,
}

/// `GROUP BY` key of step 2's `FullyWeightedEmissionRate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FullyWeightedKey {
    year_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    age_id: i32,
}

/// `GROUP BY` key of step 4-b's `FuelSupplyAdjustment`.
///
/// The SQL groups by `countyID` too, but the single-row `County` extract
/// makes it constant and the step-4-c join does not use it, so it is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FuelAdjustmentKey {
    year_id: i32,
    month_id: i32,
    pol_process_id: i32,
    model_year_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
}

/// A distinct `FuelAdjustedEmissionRate` row — the `SELECT DISTINCT` key of
/// step 4-c. Ordered so the `BTreeSet` deduplicating them is deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FuelAdjustedDistinctRow {
    year_id: i32,
    month_id: i32,
    hour_day_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
    emission_quant: OrderedRate,
}

// ===========================================================================
// Step 1 — weight emission rates by operating mode.
// ===========================================================================

/// Step 1 — `OpModeWeightedEmissionRate`.
///
/// Ports the `OpModeWeightedEmissionRateTemp` insert (the `EmissionRateRates`
/// / `HasManyOpModes` branch) plus the `OpModeWeightedEmissionRate` `GROUP BY`
/// that follows it. The temp insert's `SELECT DISTINCT` joins
/// `OpModeDistribution` ⋈ `EmissionRate` ⋈ `SourceBinDistribution` ⋈
/// `AgeCategory` (a cross join) ⋈ `SourceTypeModelYear`, projecting only the
/// five-column key plus `opModeFraction * meanBaseRate`. The
/// `SourceBinDistribution` and `SourceTypeModelYear` joins contribute no
/// projected columns — they are a pure existence filter — and the cross-joined
/// `AgeCategory` supplies the `ageGroupID` while `SourceTypeModelYear`'s
/// `modelYearID = year - ageID` predicate decides which ages survive.
///
/// Because the temp table omits `opModeID`, the subsequent `GROUP BY ... SUM`
/// sums over operating modes — and two operating modes whose rate products are
/// bit-identical for one key collapse under the `DISTINCT` and are summed only
/// once (see the module-level fidelity note). This is modeled by inserting the
/// `(key, rate)` pairs into a `BTreeSet` (the post-`DISTINCT` temp table) and
/// summing per key afterward.
fn weight_by_op_mode(
    inputs: &BasicPm25Inputs,
    constants: &RunConstants,
) -> Vec<OpModeWeightedRate> {
    // EmissionRate indexed by the `USING (polProcessID, opModeID)` join key.
    let mut er_by_pol_op: HashMap<(i32, i32), Vec<&EmissionRateRow>> = HashMap::new();
    for er in &inputs.emission_rate {
        er_by_pol_op
            .entry((er.pol_process_id, er.op_mode_id))
            .or_default()
            .push(er);
    }
    // SourceBinDistribution's `sourceTypeModelYearID`s by `(polProcessID, sourceBinID)`.
    let mut stmy_ids_by_pol_bin: HashMap<(i32, i64), Vec<i32>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        stmy_ids_by_pol_bin
            .entry((sbd.pol_process_id, sbd.source_bin_id))
            .or_default()
            .push(sbd.source_type_model_year_id);
    }
    // SourceTypeModelYear by its surrogate key.
    let stmy_by_id: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();

    // The post-`DISTINCT` temp table: a set of `(five-column key, rate)` pairs.
    let mut temp: BTreeSet<(OpModeWeightedKey, OrderedRate)> = BTreeSet::new();
    for omd in &inputs.op_mode_distribution {
        let Some(ers) = er_by_pol_op.get(&(omd.pol_process_id, omd.op_mode_id)) else {
            continue;
        };
        for er in ers {
            let Some(stmy_ids) = stmy_ids_by_pol_bin.get(&(er.pol_process_id, er.source_bin_id))
            else {
                continue;
            };
            // `opModeFraction * meanBaseRate` does not depend on the age, so it
            // is computed once for the `(omd, er)` pair.
            let rate = omd.op_mode_fraction * er.mean_base_rate;
            for acat in &inputs.age_category {
                // The existence filter: some `SourceBinDistribution` row must
                // reach a `SourceTypeModelYear` whose source type matches the
                // `OpModeDistribution` row and whose model year is `year - ageID`.
                let target_model_year = constants.year - acat.age_id;
                let exists = stmy_ids.iter().any(|id| {
                    stmy_by_id.get(id).is_some_and(|stmy| {
                        stmy.source_type_id == omd.source_type_id
                            && stmy.model_year_id == target_model_year
                    })
                });
                if !exists {
                    continue;
                }
                let key = OpModeWeightedKey {
                    hour_day_id: omd.hour_day_id,
                    source_type_id: omd.source_type_id,
                    source_bin_id: er.source_bin_id,
                    age_group_id: acat.age_group_id,
                    pol_process_id: omd.pol_process_id,
                };
                temp.insert((key, OrderedRate(rate)));
            }
        }
    }

    // `OpModeWeightedEmissionRate`: `GROUP BY` the five-column key, `SUM` the
    // distinct rates the temp table holds for it.
    let mut acc: BTreeMap<OpModeWeightedKey, f64> = BTreeMap::new();
    for (key, rate) in temp {
        *acc.entry(key).or_insert(0.0) += rate.0;
    }
    acc.into_iter()
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

// ===========================================================================
// Step 2 — weight emission rates by source bin.
// ===========================================================================

/// Step 2 — `FullyWeightedEmissionRate`.
///
/// Sums `sourceBinActivityFraction * opModeWeightedMeanBaseRate` over the
/// `OpModeWeightedEmissionRate` ⋈ `SourceBinDistribution` ⋈ `AgeCategory` ⋈
/// `SourceTypeModelYear` ⋈ `PollutantProcessModelYear` ⋈ `SourceBin` join,
/// grouped by `(hourDayID, sourceTypeID, fuelTypeID, modelYearID, polProcessID,
/// ageID)`. Every projected non-aggregate column is in the `GROUP BY`, so —
/// unlike `BasicStartPM25Calculator.sql` — there is no loose-`GROUP BY`
/// indeterminacy here.
///
/// `SourceTypeModelYear` (keyed by its surrogate id), `PollutantProcessModelYear`
/// and `SourceBin` each contribute at most one matching row per
/// `(OpModeWeightedEmissionRate, SourceBinDistribution)` pair, and the
/// `AgeCategory` join collapses to the single `ageID = year - modelYearID`, so
/// each pair contributes exactly one term to its group's sum.
fn weight_by_source_bin(
    op_mode_weighted: &[OpModeWeightedRate],
    inputs: &BasicPm25Inputs,
    constants: &RunConstants,
) -> Vec<FullyWeightedRate> {
    // SourceBinDistribution rows by `(polProcessID, sourceBinID)`.
    let mut sbd_by_pol_bin: HashMap<(i32, i64), Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        sbd_by_pol_bin
            .entry((sbd.pol_process_id, sbd.source_bin_id))
            .or_default()
            .push(sbd);
    }
    let stmy_by_id: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();
    // AgeCategory as the set of `(ageGroupID, ageID)` pairs it defines.
    let age_pairs: HashSet<(i32, i32)> = inputs
        .age_category
        .iter()
        .map(|acat| (acat.age_group_id, acat.age_id))
        .collect();
    let ppmy_by: HashMap<(i32, i32), &PollutantProcessModelYearRow> = inputs
        .pollutant_process_model_year
        .iter()
        .map(|ppmy| ((ppmy.pol_process_id, ppmy.model_year_id), ppmy))
        .collect();
    let sb_by_id: HashMap<i64, &SourceBinRow> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb))
        .collect();

    let mut acc: BTreeMap<FullyWeightedKey, f64> = BTreeMap::new();
    for omer in op_mode_weighted {
        let Some(sbds) = sbd_by_pol_bin.get(&(omer.pol_process_id, omer.source_bin_id)) else {
            continue;
        };
        for sbd in sbds {
            let Some(stmy) = stmy_by_id.get(&sbd.source_type_model_year_id) else {
                continue;
            };
            if stmy.source_type_id != omer.source_type_id {
                continue;
            }
            // `AgeCategory` join: `ageID = year - modelYearID` must exist and
            // belong to this rate's age group.
            let age_id = constants.year - stmy.model_year_id;
            if !age_pairs.contains(&(omer.age_group_id, age_id)) {
                continue;
            }
            let Some(ppmy) = ppmy_by.get(&(omer.pol_process_id, stmy.model_year_id)) else {
                continue;
            };
            let Some(sb) = sb_by_id.get(&omer.source_bin_id) else {
                continue;
            };
            if sb.model_year_group_id != ppmy.model_year_group_id {
                continue;
            }
            let key = FullyWeightedKey {
                year_id: constants.year,
                hour_day_id: omer.hour_day_id,
                source_type_id: omer.source_type_id,
                fuel_type_id: sb.fuel_type_id,
                model_year_id: stmy.model_year_id,
                pol_process_id: omer.pol_process_id,
                age_id,
            };
            *acc.entry(key).or_insert(0.0) +=
                sbd.source_bin_activity_fraction * omer.op_mode_weighted_mean_base_rate;
        }
    }
    acc.into_iter()
        .map(|(key, sum)| FullyWeightedRate {
            year_id: key.year_id,
            hour_day_id: key.hour_day_id,
            source_type_id: key.source_type_id,
            fuel_type_id: key.fuel_type_id,
            model_year_id: key.model_year_id,
            pol_process_id: key.pol_process_id,
            fully_weighted_mean_base_rate: sum,
            age_id: key.age_id,
        })
        .collect()
}

// ===========================================================================
// Step 3 — multiply emission rates by activity.
// ===========================================================================

/// Step 3 — `UnadjustedEmissionResults`.
///
/// Ports the `SourceHoursOperatingActivity` branch: multiplies each
/// fully-weighted rate by the matching `SHO` count, joined `USING (hourDayID,
/// yearID, ageID, sourceTypeID)`. A rate with several `SHO` months produces
/// one unadjusted-emission row per month. The SQL's `insert ignore` targets a
/// table with no unique key, so no rows are dropped — this is a plain
/// fan-out, not a dedup.
fn multiply_by_activity(
    fully_weighted: &[FullyWeightedRate],
    inputs: &BasicPm25Inputs,
) -> Vec<UnadjustedEmission> {
    let mut sho_by: HashMap<(i32, i32, i32, i32), Vec<&ShoRow>> = HashMap::new();
    for s in &inputs.sho {
        sho_by
            .entry((s.hour_day_id, s.year_id, s.age_id, s.source_type_id))
            .or_default()
            .push(s);
    }

    let mut out = Vec::new();
    for f in fully_weighted {
        let Some(sho_rows) = sho_by.get(&(f.hour_day_id, f.year_id, f.age_id, f.source_type_id))
        else {
            continue;
        };
        for s in sho_rows {
            out.push(UnadjustedEmission {
                year_id: f.year_id,
                month_id: s.month_id,
                hour_day_id: f.hour_day_id,
                source_type_id: f.source_type_id,
                fuel_type_id: f.fuel_type_id,
                model_year_id: f.model_year_id,
                pol_process_id: f.pol_process_id,
                unadjusted_emission_quant: f.fully_weighted_mean_base_rate * s.sho,
            });
        }
    }
    out
}

// ===========================================================================
// Step 4 — weight emission rates by fuel adjustment.
// ===========================================================================

/// Step 4-b, part 1 — `FuelSupplyWithFuelType`.
///
/// Joins `FuelSupply` ⋈ `FuelFormulation` ⋈ `FuelSubType` ⋈ `MonthOfAnyYear`
/// ⋈ `Year`, keeping only the fuel supply for the run year. `MonthOfAnyYear`
/// fans a fuel supply's month *group* out to one row per month in it.
fn build_fuel_supply_with_fuel_type(
    inputs: &BasicPm25Inputs,
    constants: &RunConstants,
) -> Vec<FuelSupplyWithFuelType> {
    // FuelFormulation -> fuelSubtypeID; FuelSubType -> fuelTypeID.
    let sub_type_of: HashMap<i32, i32> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff.fuel_sub_type_id))
        .collect();
    let fuel_type_of: HashMap<i32, i32> = inputs
        .fuel_sub_type
        .iter()
        .map(|fst| (fst.fuel_sub_type_id, fst.fuel_type_id))
        .collect();
    // MonthOfAnyYear: monthGroupID -> [monthID].
    let mut months_of_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for may in &inputs.month_of_any_year {
        months_of_group
            .entry(may.month_group_id)
            .or_default()
            .push(may.month_id);
    }
    // Year filtered to the run year -> the set of fuel years it maps to (the
    // `Year` extract is `WHERE yearID = year`, so this is normally one value).
    let run_fuel_years: Vec<i32> = inputs
        .year
        .iter()
        .filter(|y| y.year_id == constants.year)
        .map(|y| y.fuel_year_id)
        .collect();

    let mut out = Vec::new();
    for fs in &inputs.fuel_supply {
        if !run_fuel_years.contains(&fs.fuel_year_id) {
            continue;
        }
        let Some(&sub_type_id) = sub_type_of.get(&fs.fuel_formulation_id) else {
            continue;
        };
        let Some(&fuel_type_id) = fuel_type_of.get(&sub_type_id) else {
            continue;
        };
        let Some(months) = months_of_group.get(&fs.month_group_id) else {
            continue;
        };
        for &month_id in months {
            out.push(FuelSupplyWithFuelType {
                year_id: constants.year,
                month_id,
                fuel_formulation_id: fs.fuel_formulation_id,
                fuel_type_id,
                market_share: fs.market_share,
            });
        }
    }
    out
}

/// Step 4-b, part 2 — `FuelSupplyAdjustment`.
///
/// For every `(PollutantProcessModelYear × RunSpecSourceType ×
/// FuelSupplyWithFuelType)` combination — the SQL cross-joins them — sums
/// `(ifnull(fuelEffectRatio, 1) + GPAFract * (ifnull(fuelEffectRatioGPA, 1) -
/// ifnull(fuelEffectRatio, 1))) * marketShare` over the `LEFT JOIN` to
/// `generalFuelRatio`, grouped by `(yearID, monthID, polProcessID, modelYearID,
/// sourceTypeID, fuelTypeID)`. A combination with no `generalFuelRatio` match
/// keeps the `LEFT JOIN`'s `NULL` row, where both ratios `ifnull` to `1.0` and
/// the term reduces to `marketShare`.
fn build_fuel_supply_adjustment(
    fuel_supply_wft: &[FuelSupplyWithFuelType],
    inputs: &BasicPm25Inputs,
    constants: &RunConstants,
) -> HashMap<FuelAdjustmentKey, f64> {
    let mut acc: HashMap<FuelAdjustmentKey, f64> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        let age = constants.year - ppmy.model_year_id;
        for rst in &inputs.run_spec_source_type {
            for fsft in fuel_supply_wft {
                let key = FuelAdjustmentKey {
                    year_id: fsft.year_id,
                    month_id: fsft.month_id,
                    pol_process_id: ppmy.pol_process_id,
                    model_year_id: ppmy.model_year_id,
                    source_type_id: rst.source_type_id,
                    fuel_type_id: fsft.fuel_type_id,
                };
                let mut matched = false;
                for gfr in &inputs.general_fuel_ratio {
                    if gfr.fuel_formulation_id == fsft.fuel_formulation_id
                        && gfr.pol_process_id == ppmy.pol_process_id
                        && gfr.min_model_year_id <= ppmy.model_year_id
                        && gfr.max_model_year_id >= ppmy.model_year_id
                        && gfr.min_age_id <= age
                        && gfr.max_age_id >= age
                        && gfr.source_type_id == rst.source_type_id
                    {
                        matched = true;
                        let fer = gfr.fuel_effect_ratio.unwrap_or(1.0);
                        let fer_gpa = gfr.fuel_effect_ratio_gpa.unwrap_or(1.0);
                        let term =
                            (fer + constants.gpa_fract * (fer_gpa - fer)) * fsft.market_share;
                        *acc.entry(key).or_insert(0.0) += term;
                    }
                }
                if !matched {
                    // The `LEFT JOIN` `NULL` row: both ratios `ifnull` to 1, so
                    // `(1 + GPAFract * (1 - 1)) * marketShare = marketShare`.
                    *acc.entry(key).or_insert(0.0) += fsft.market_share;
                }
            }
        }
    }
    acc
}

/// Step 4-c — `FuelAdjustedEmissionRate`.
///
/// Joins each `UnadjustedEmissionResults` row to its `FuelSupplyAdjustment`
/// (an inner join on `(yearID, monthID, sourceTypeID, fuelTypeID, modelYearID,
/// polProcessID)` — a row with no adjustment is dropped) and scales the
/// quantity by `fuelAdjustment`. The SQL `coalesce(fuelAdjustment * quant,
/// quant)` falls back to the unadjusted quantity when `fuelAdjustment` is
/// `NULL`; here `marketShare` and `GPAFract` are modeled as non-null, so the
/// summed `fuelAdjustment` is always a real number and the product is taken
/// directly.
///
/// The SQL's `SELECT DISTINCT` deduplicates `(seven-column key, quant)` rows —
/// `UnadjustedEmissionResults` can hold several rows per key, one per vehicle
/// age. This port reproduces that with a `BTreeSet`, which also fixes a
/// deterministic output order.
fn apply_fuel_adjustment(
    unadjusted: &[UnadjustedEmission],
    fuel_adjustment: &HashMap<FuelAdjustmentKey, f64>,
) -> Vec<FuelAdjustedRate> {
    let mut distinct: BTreeSet<FuelAdjustedDistinctRow> = BTreeSet::new();
    for u in unadjusted {
        let key = FuelAdjustmentKey {
            year_id: u.year_id,
            month_id: u.month_id,
            pol_process_id: u.pol_process_id,
            model_year_id: u.model_year_id,
            source_type_id: u.source_type_id,
            fuel_type_id: u.fuel_type_id,
        };
        let Some(&adjustment) = fuel_adjustment.get(&key) else {
            continue;
        };
        distinct.insert(FuelAdjustedDistinctRow {
            year_id: u.year_id,
            month_id: u.month_id,
            hour_day_id: u.hour_day_id,
            source_type_id: u.source_type_id,
            fuel_type_id: u.fuel_type_id,
            model_year_id: u.model_year_id,
            pol_process_id: u.pol_process_id,
            emission_quant: OrderedRate(adjustment * u.unadjusted_emission_quant),
        });
    }
    distinct
        .into_iter()
        .map(|r| FuelAdjustedRate {
            year_id: r.year_id,
            month_id: r.month_id,
            hour_day_id: r.hour_day_id,
            source_type_id: r.source_type_id,
            fuel_type_id: r.fuel_type_id,
            model_year_id: r.model_year_id,
            pol_process_id: r.pol_process_id,
            emission_quant: r.emission_quant.0,
        })
        .collect()
}

// ===========================================================================
// Step 5 — apply temperature adjustment (NoTemperatureAdjustment).
// ===========================================================================

/// Step 5 — `AdjustedEmissionResults`, the `NoTemperatureAdjustment` branch.
///
/// Brake-wear and tire-wear PM2.5 enable the `NoTemperatureAdjustment`
/// section, whose insert copies `unadjustedEmissionQuant` straight into
/// `emissionQuant` — there is no temperature correction. The only work is the
/// `HourDay` join that decodes `hourDayID` into `(dayID, hourID)`. The
/// script's `ApplyTemperatureAdjustment` and `ApplyLinearTemperatureAdjustment`
/// branches are not enabled for these calculators and are not ported.
fn decode_hour_day(
    fuel_adjusted: &[FuelAdjustedRate],
    inputs: &BasicPm25Inputs,
) -> Vec<AdjustedEmission> {
    let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();

    let mut out = Vec::new();
    for fa in fuel_adjusted {
        let Some(hd) = hour_day_by_id.get(&fa.hour_day_id) else {
            continue;
        };
        out.push(AdjustedEmission {
            year_id: fa.year_id,
            month_id: fa.month_id,
            day_id: hd.day_id,
            hour_id: hd.hour_id,
            source_type_id: fa.source_type_id,
            fuel_type_id: fa.fuel_type_id,
            model_year_id: fa.model_year_id,
            pol_process_id: fa.pol_process_id,
            emission_quant: fa.emission_quant,
        });
    }
    out
}

// ===========================================================================
// Step 6 — convert to MOVESWorkerOutput.
// ===========================================================================

/// Step 6 — the `MOVESWorkerOutput` insert.
///
/// Splits `polProcessID` into `(pollutantID, processID)` through
/// `PollutantProcessAssoc` and attaches the constant location columns
/// (`stateID`, `countyID`, `zoneID`, `linkID` from `##context...##`, and
/// `roadTypeID` from the cross-joined single-row `Link` extract). An
/// `AdjustedEmissionResults` row whose `polProcessID` has no
/// `PollutantProcessAssoc` entry is dropped by the inner join.
fn to_worker_output(
    adjusted: &[AdjustedEmission],
    inputs: &BasicPm25Inputs,
    constants: &RunConstants,
) -> Vec<WorkerOutputRow> {
    let ppa_by: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|ppa| (ppa.pol_process_id, ppa))
        .collect();

    let mut out = Vec::new();
    for a in adjusted {
        let Some(ppa) = ppa_by.get(&a.pol_process_id) else {
            continue;
        };
        out.push(WorkerOutputRow {
            year_id: a.year_id,
            month_id: a.month_id,
            day_id: a.day_id,
            hour_id: a.hour_id,
            state_id: constants.state_id,
            county_id: constants.county_id,
            zone_id: constants.zone_id,
            link_id: constants.link_id,
            pollutant_id: ppa.pollutant_id,
            process_id: ppa.process_id,
            source_type_id: a.source_type_id,
            fuel_type_id: a.fuel_type_id,
            model_year_id: a.model_year_id,
            road_type_id: constants.road_type_id,
            emission_quant: a.emission_quant,
        });
    }
    out
}

// ===========================================================================
// The shared algorithm.
// ===========================================================================

/// The shared `database/BasicPM25Calculator.sql` algorithm.
///
/// `BasicBrakeWearPMEmissionCalculator` and `BasicTireWearPMEmissionCalculator`
/// are distinct Java `GenericCalculatorBase` subclasses, but both name the
/// **same** SQL script with the **same** four enabled sections, so their
/// compute core is identical — only the `polProcessID` flowing through the
/// extracted tables differs. This zero-sized type carries that shared core as
/// [`run`](BasicPm25Calculator::run); the two [`Calculator`] implementors —
/// [`BasicBrakeWearPmEmissionCalculator`] and [`BasicTireWearPmEmissionCalculator`]
/// — hold only the differing subscription metadata and delegate their numerics
/// here. It is itself **not** a [`Calculator`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicPm25Calculator;

impl BasicPm25Calculator {
    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Chains the six processing steps of `BasicPM25Calculator.sql` and
    /// returns the `MOVESWorkerOutput` rows the SQL would insert. Output rows
    /// are ordered deterministically by the step-4-c `DISTINCT` `BTreeSet`,
    /// which steps 5 and 6 preserve.
    #[must_use]
    pub fn run(inputs: &BasicPm25Inputs, constants: &RunConstants) -> Vec<WorkerOutputRow> {
        let op_mode_weighted = weight_by_op_mode(inputs, constants);
        let fully_weighted = weight_by_source_bin(&op_mode_weighted, inputs, constants);
        let unadjusted = multiply_by_activity(&fully_weighted, inputs);
        let fuel_supply_wft = build_fuel_supply_with_fuel_type(inputs, constants);
        let fuel_adjustment = build_fuel_supply_adjustment(&fuel_supply_wft, inputs, constants);
        let fuel_adjusted = apply_fuel_adjustment(&unadjusted, &fuel_adjustment);
        let adjusted = decode_hour_day(&fuel_adjusted, inputs);
        to_worker_output(&adjusted, inputs, constants)
    }
}

// ===========================================================================
// Shared `Calculator` metadata.
// ===========================================================================

/// The `(pollutant, process)` pairs the brake/tire calculators register —
/// **none**, for either one.
///
/// `BasicBrakeWearPMEmissionCalculator` and `BasicTireWearPMEmissionCalculator`
/// are superseded by `BaseRateCalculator` (see the module-level supersession
/// note): both are absent from `CalculatorInfo.txt` and
/// `calculator-dag.json` records `registrations_count: 0` for each. The
/// brake-wear `(116, 9)` and tire-wear `(117, 10)` pairs are registered to
/// `BaseRateCalculator`, so registering them here too would double-register
/// them in the calculator registry. The Java constructors' legacy
/// `EmissionCalculatorRegistration.register(...)` calls are intentionally not
/// ported.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[];

// ===========================================================================
// Data-plane wiring — TableRow impls for all input / output row types.
// ===========================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction { table: table.into(), row, column: column.into(), message: msg }
}

/// Minimal Link extract — only `roadTypeID` is needed to populate
/// [`RunConstants::road_type_id`].
struct LinkRow {
    road_type_id: i32,
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str { "Link" }
    fn polars_schema() -> Schema {
        Schema::from_iter([("roadTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let road_type_id = get_i32("roadTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(LinkRow { road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? })
        }).collect()
    }
}

/// Minimal County extract — only `GPAFract` feeds the algorithm.
struct CountyRow {
    county_id: i32,
    gpa_fract: f64,
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

impl TableRow for OpModeDistributionRow {
    fn table_name() -> &'static str { "OpModeDistribution" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
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
        let hour_day_id = get_i32("hourDayID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(OpModeDistributionRow {
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                op_mode_fraction: op_mode_fraction.get(i).ok_or_else(|| null("opModeFraction"))?,
            })
        }).collect()
    }
}

impl TableRow for EmissionRateRow {
    fn table_name() -> &'static str { "EmissionRate" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceBinID".into(), DataType::Int64),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("opModeID".into(), rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>()).into(),
            Series::new("meanBaseRate".into(), rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionRate";
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
        let mean_base_rate = get_f64("meanBaseRate")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(EmissionRateRow {
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
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
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let model_year_id = get_i32("modelYearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceTypeModelYearRow {
                source_type_model_year_id: source_type_model_year_id.get(i).ok_or_else(|| null("sourceTypeModelYearID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
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
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_id = get_i32("modelYearID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessModelYearRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                model_year_group_id: model_year_group_id.get(i).ok_or_else(|| null("modelYearGroupID"))?,
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
            ("modelYearGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
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
        let model_year_group_id = get_i32("modelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinRow {
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_group_id: model_year_group_id.get(i).ok_or_else(|| null("modelYearGroupID"))?,
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

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str { "FuelSupply" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
            Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
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
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let month_group_id = get_i32("monthGroupID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSupplyRow {
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
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
            Series::new("fuelSubtypeID".into(), rows.iter().map(|r| r.fuel_sub_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let fuel_sub_type_id = get_i32("fuelSubtypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelFormulationRow {
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                fuel_sub_type_id: fuel_sub_type_id.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelSubTypeRow {
    fn table_name() -> &'static str { "FuelSubType" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("fuelSubtypeID".into(), rows.iter().map(|r| r.fuel_sub_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSubType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_sub_type_id = get_i32("fuelSubtypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSubTypeRow {
                fuel_sub_type_id: fuel_sub_type_id.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
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

impl TableRow for RunSpecSourceTypeRow {
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
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(RunSpecSourceTypeRow {
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for GeneralFuelRatioRow {
    fn table_name() -> &'static str { "generalFuelRatio" }
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
        DataFrame::new(n, vec![
            Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("minModelYearID".into(), rows.iter().map(|r| r.min_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("maxModelYearID".into(), rows.iter().map(|r| r.max_model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("minAgeID".into(), rows.iter().map(|r| r.min_age_id).collect::<Vec<i32>>()).into(),
            Series::new("maxAgeID".into(), rows.iter().map(|r| r.max_age_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelEffectRatio".into(), rows.iter().map(|r| r.fuel_effect_ratio).collect::<Vec<Option<f64>>>()).into(),
            Series::new("fuelEffectRatioGPA".into(), rows.iter().map(|r| r.fuel_effect_ratio_gpa).collect::<Vec<Option<f64>>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "generalFuelRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64_opt = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let min_age_id = get_i32("minAgeID")?;
        let max_age_id = get_i32("maxAgeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_effect_ratio = get_f64_opt("fuelEffectRatio")?;
        let fuel_effect_ratio_gpa = get_f64_opt("fuelEffectRatioGPA")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(GeneralFuelRatioRow {
                fuel_formulation_id: fuel_formulation_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                min_model_year_id: min_model_year_id.get(i).ok_or_else(|| null("minModelYearID"))?,
                max_model_year_id: max_model_year_id.get(i).ok_or_else(|| null("maxModelYearID"))?,
                min_age_id: min_age_id.get(i).ok_or_else(|| null("minAgeID"))?,
                max_age_id: max_age_id.get(i).ok_or_else(|| null("maxAgeID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_effect_ratio: fuel_effect_ratio.get(i),
                fuel_effect_ratio_gpa: fuel_effect_ratio_gpa.get(i),
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

impl TableRow for WorkerOutputRow {
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
            Ok(WorkerOutputRow {
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

/// Default-DB tables `BasicPM25Calculator.sql`'s processing pass reads —
/// shared by both calculators.
///
/// The script's extract section also pulls `County`, `Zone`, `Pollutant`,
/// `EmissionProcess`, `RunSpecMonth` and the rate/temperature tables for
/// sections this calculator pair does not enable; only the tables the
/// `Section Processing` pass actually consumes are listed. `County` and `Link`
/// appear because the single-row extracts of each supply the `GPAFract` and
/// `roadTypeID` run constants.
static INPUT_TABLES: &[&str] = &[
    "OpModeDistribution",
    "EmissionRate",
    "SourceBinDistribution",
    "AgeCategory",
    "SourceTypeModelYear",
    "PollutantProcessModelYear",
    "SourceBin",
    "SHO",
    "HourDay",
    "FuelSupply",
    "FuelFormulation",
    "FuelSubType",
    "MonthOfAnyYear",
    "Year",
    "RunSpecSourceType",
    "generalFuelRatio",
    "PollutantProcessAssoc",
    "County",
    "Link",
];

/// Build a one-entry subscription slice for the given process.
///
/// `GenericCalculatorBase.subscribeToMe` subscribes once per process the
/// calculator's `polProcessID`s span; each brake/tire calculator names a
/// single `polProcessID` in one process, so there is exactly one
/// subscription, at `YEAR` granularity and `EMISSION_CALCULATOR` priority (the
/// Java constructors pass a zero priority offset). `calculator-dag.json`
/// records the granularity and priority but a placeholder `process_id` of 0,
/// because the static analyser cannot resolve `GenericCalculatorBase`'s
/// runtime `polProcessID` lookup — the true process id comes from the
/// constructor's `polProcessID`.
fn subscription_for(process_id: u16) -> [CalculatorSubscription; 1] {
    let priority =
        Priority::parse("EMISSION_CALCULATOR").expect("EMISSION_CALCULATOR is a valid priority");
    [CalculatorSubscription::new(
        ProcessId(process_id),
        Granularity::Year,
        priority,
    )]
}

/// Build `(BasicPm25Inputs, RunConstants)` from a [`CalculatorContext`].
///
/// Shared by both brake-wear and tire-wear `execute` bodies; reads all
/// required tables from `ctx.tables()` and derives run constants from
/// `ctx.position()` plus the single-row `County` and `Link` extracts.
fn build_inputs(ctx: &CalculatorContext) -> Result<(BasicPm25Inputs, RunConstants), Error> {
    let tables = ctx.tables();
    let pos = ctx.position();
    let link_rows: Vec<LinkRow> = tables.iter_typed("Link")?;
    let road_type_id = link_rows.first().map(|r| r.road_type_id).unwrap_or(0);
    let county_rows: Vec<CountyRow> = tables.iter_typed("County")?;
    let gpa_fract = county_rows.first().map(|r| r.gpa_fract).unwrap_or(0.0);
    let constants = RunConstants {
        year: pos.time.year.map(|y| y as i32).unwrap_or(0),
        state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
        county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
        zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
        link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
        road_type_id,
        gpa_fract,
    };
    let inputs = BasicPm25Inputs {
        op_mode_distribution: tables.iter_typed("OpModeDistribution")?,
        emission_rate: tables.iter_typed("EmissionRate")?,
        source_bin_distribution: tables.iter_typed("SourceBinDistribution")?,
        age_category: tables.iter_typed("AgeCategory")?,
        source_type_model_year: tables.iter_typed("SourceTypeModelYear")?,
        pollutant_process_model_year: tables.iter_typed("PollutantProcessModelYear")?,
        source_bin: tables.iter_typed("SourceBin")?,
        sho: tables.iter_typed("SHO")?,
        hour_day: tables.iter_typed("HourDay")?,
        fuel_supply: tables.iter_typed("FuelSupply")?,
        fuel_formulation: tables.iter_typed("FuelFormulation")?,
        fuel_sub_type: tables.iter_typed("FuelSubType")?,
        month_of_any_year: tables.iter_typed("MonthOfAnyYear")?,
        year: tables.iter_typed("Year")?,
        run_spec_source_type: tables.iter_typed("RunSpecSourceType")?,
        general_fuel_ratio: tables.iter_typed("generalFuelRatio")?,
        pollutant_process_assoc: tables.iter_typed("PollutantProcessAssoc")?,
    };
    Ok((inputs, constants))
}

// ===========================================================================
// The brake-wear calculator.
// ===========================================================================

/// The Basic Brake Wear PM Emission Calculator — `BasicBrakeWearPMEmissionCalculator`.
///
/// Computes brake-wear PM2.5 (pollutant 116) for the Brakewear process
/// (process 9). A zero-sized value type owning no per-run state, as the
/// [`Calculator`] trait requires; all run-varying input flows through
/// [`BasicPm25Calculator::run`], which this calculator delegates to.
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicBrakeWearPmEmissionCalculator;

impl BasicBrakeWearPmEmissionCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = "BasicBrakeWearPMEmissionCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// The brake-wear calculator's master-loop subscription — Brakewear (process
/// 9), `YEAR`, `EMISSION_CALCULATOR`.
fn brakewear_subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| subscription_for(BRAKEWEAR_PROCESS_ID))
}

impl Calculator for BasicBrakeWearPmEmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        brakewear_subscriptions()
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let (inputs, constants) = build_inputs(ctx)?;
        let rows = BasicPm25Calculator::run(&inputs, &constants);
        crate::wiring::emit_rows(rows)
    }
}

// ===========================================================================
// The tire-wear calculator.
// ===========================================================================

/// The Basic Tire Wear PM Emission Calculator — `BasicTireWearPMEmissionCalculator`.
///
/// Computes tire-wear PM2.5 (pollutant 117) for the Tirewear process
/// (process 10). A zero-sized value type owning no per-run state, as the
/// [`Calculator`] trait requires; all run-varying input flows through
/// [`BasicPm25Calculator::run`], which this calculator delegates to.
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicTireWearPmEmissionCalculator;

impl BasicTireWearPmEmissionCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = "BasicTireWearPMEmissionCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// The tire-wear calculator's master-loop subscription — Tirewear (process
/// 10), `YEAR`, `EMISSION_CALCULATOR`.
fn tirewear_subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| subscription_for(TIREWEAR_PROCESS_ID))
}

impl Calculator for BasicTireWearPmEmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        tirewear_subscriptions()
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let (inputs, constants) = build_inputs(ctx)?;
        let rows = BasicPm25Calculator::run(&inputs, &constants);
        crate::wiring::emit_rows(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `polProcessID` for brake-wear PM2.5, Brakewear process (Java `"11609"`).
    const BRAKEWEAR_POL_PROCESS_ID: i32 = 11609;
    /// `polProcessID` for tire-wear PM2.5, Tirewear process (Java `"11710"`).
    const TIREWEAR_POL_PROCESS_ID: i32 = 11710;
    /// MOVES pollutant id for Primary PM2.5 — Brakewear Particulate.
    const BRAKEWEAR_POLLUTANT_ID: i32 = 116;
    /// MOVES pollutant id for Primary PM2.5 — Tirewear Particulate.
    const TIREWEAR_POLLUTANT_ID: i32 = 117;

    /// A minimal set of inputs that flows one emission cleanly through all six
    /// steps for the given `polProcessID` / `(pollutant, process)`.
    ///
    /// `year = 2020`, one source type (21), one source bin (100), age 0
    /// (model year 2020), one fuel formulation, no `generalFuelRatio` rows —
    /// so the fuel adjustment reduces to the formulation's `marketShare`.
    fn single_flow(
        pol_process_id: i32,
        pollutant_id: i32,
        process_id: i32,
    ) -> (BasicPm25Inputs, RunConstants) {
        let inputs = BasicPm25Inputs {
            op_mode_distribution: vec![OpModeDistributionRow {
                hour_day_id: 1,
                source_type_id: 21,
                pol_process_id,
                op_mode_id: 300,
                op_mode_fraction: 0.5,
            }],
            emission_rate: vec![EmissionRateRow {
                source_bin_id: 100,
                pol_process_id,
                op_mode_id: 300,
                mean_base_rate: 4.0,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 500,
                pol_process_id,
                source_bin_id: 100,
                source_bin_activity_fraction: 1.0,
            }],
            age_category: vec![AgeCategoryRow {
                age_id: 0,
                age_group_id: 3,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 500,
                model_year_id: 2020,
                source_type_id: 21,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id,
                model_year_id: 2020,
                model_year_group_id: 7,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 100,
                fuel_type_id: 1,
                model_year_group_id: 7,
            }],
            sho: vec![ShoRow {
                hour_day_id: 1,
                month_id: 7,
                year_id: 2020,
                age_id: 0,
                source_type_id: 21,
                sho: 10.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 1,
                day_id: 5,
                hour_id: 8,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_formulation_id: 9000,
                month_group_id: 1,
                fuel_year_id: 2020,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 9000,
                fuel_sub_type_id: 10,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 10,
                fuel_type_id: 1,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 1,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            general_fuel_ratio: Vec::new(),
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id,
                pollutant_id,
                process_id,
            }],
        };
        let constants = RunConstants {
            year: 2020,
            state_id: 8,
            county_id: 8001,
            zone_id: 80010,
            link_id: 800_101,
            road_type_id: 5,
            gpa_fract: 0.0,
        };
        (inputs, constants)
    }

    /// The brake-wear `single_flow` fixture.
    fn brakewear_flow() -> (BasicPm25Inputs, RunConstants) {
        single_flow(BRAKEWEAR_POL_PROCESS_ID, BRAKEWEAR_POLLUTANT_ID, 9)
    }

    #[test]
    fn brakewear_metadata_matches_the_dag_entry() {
        let calc = BasicBrakeWearPmEmissionCalculator::new();
        assert_eq!(calc.name(), "BasicBrakeWearPMEmissionCalculator");

        // One subscription: Brakewear (process 9), YEAR, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(9));
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

        // No registrations — superseded by BaseRateCalculator, which owns the
        // (116, 9) pair (calculator-dag.json: registrations_count 0).
        assert!(calc.registrations().is_empty());
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"EmissionRate"));
        assert!(calc.input_tables().contains(&"SHO"));
    }

    #[test]
    fn tirewear_metadata_matches_the_dag_entry() {
        let calc = BasicTireWearPmEmissionCalculator::new();
        assert_eq!(calc.name(), "BasicTireWearPMEmissionCalculator");

        // One subscription: Tirewear (process 10), YEAR, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(10));
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

        // No registrations — superseded by BaseRateCalculator, which owns the
        // (117, 10) pair (calculator-dag.json: registrations_count 0).
        assert!(calc.registrations().is_empty());
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"FuelSupply"));
    }

    fn make_store(inputs: &BasicPm25Inputs, constants: &RunConstants) -> moves_framework::InMemoryStore {
        use moves_framework::DataFrameStore;
        let mut store = moves_framework::InMemoryStore::new();
        store.insert("Link", LinkRow::into_dataframe(vec![LinkRow { road_type_id: constants.road_type_id }]).unwrap());
        store.insert("County", CountyRow::into_dataframe(vec![CountyRow { county_id: constants.county_id, gpa_fract: constants.gpa_fract }]).unwrap());
        store.insert("OpModeDistribution", OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution.clone()).unwrap());
        store.insert("EmissionRate", EmissionRateRow::into_dataframe(inputs.emission_rate.clone()).unwrap());
        store.insert("SourceBinDistribution", SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution.clone()).unwrap());
        store.insert("AgeCategory", AgeCategoryRow::into_dataframe(inputs.age_category.clone()).unwrap());
        store.insert("SourceTypeModelYear", SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year.clone()).unwrap());
        store.insert("PollutantProcessModelYear", PollutantProcessModelYearRow::into_dataframe(inputs.pollutant_process_model_year.clone()).unwrap());
        store.insert("SourceBin", SourceBinRow::into_dataframe(inputs.source_bin.clone()).unwrap());
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho.clone()).unwrap());
        store.insert("HourDay", HourDayRow::into_dataframe(inputs.hour_day.clone()).unwrap());
        store.insert("FuelSupply", FuelSupplyRow::into_dataframe(inputs.fuel_supply.clone()).unwrap());
        store.insert("FuelFormulation", FuelFormulationRow::into_dataframe(inputs.fuel_formulation.clone()).unwrap());
        store.insert("FuelSubType", FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type.clone()).unwrap());
        store.insert("MonthOfAnyYear", MonthOfAnyYearRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap());
        store.insert("Year", YearRow::into_dataframe(inputs.year.clone()).unwrap());
        store.insert("RunSpecSourceType", RunSpecSourceTypeRow::into_dataframe(inputs.run_spec_source_type.clone()).unwrap());
        store.insert("generalFuelRatio", GeneralFuelRatioRow::into_dataframe(inputs.general_fuel_ratio.clone()).unwrap());
        store.insert("PollutantProcessAssoc", PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc.clone()).unwrap());
        store
    }

    #[test]
    fn brakewear_execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let (inputs, constants) = brakewear_flow();
        let store = make_store(&inputs, &constants);
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(
                constants.state_id as u32,
                constants.county_id as u32,
                constants.zone_id as u32,
                constants.link_id as u32,
            ),
            time: ExecutionTime::year(constants.year as u16),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = BasicBrakeWearPmEmissionCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(out.dataframe().unwrap().height() > 0, "expected at least one row");
    }

    #[test]
    fn tirewear_execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let (inputs, constants) = single_flow(TIREWEAR_POL_PROCESS_ID, TIREWEAR_POLLUTANT_ID, 10);
        let store = make_store(&inputs, &constants);
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(
                constants.state_id as u32,
                constants.county_id as u32,
                constants.zone_id as u32,
                constants.link_id as u32,
            ),
            time: ExecutionTime::year(constants.year as u16),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = BasicTireWearPmEmissionCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(out.dataframe().unwrap().height() > 0, "expected at least one row");
    }

    #[test]
    fn calculators_are_object_safe() {
        let calcs: Vec<Box<dyn Calculator>> = vec![
            Box::new(BasicBrakeWearPmEmissionCalculator::new()),
            Box::new(BasicTireWearPmEmissionCalculator::new()),
        ];
        assert_eq!(calcs[0].name(), "BasicBrakeWearPMEmissionCalculator");
        assert_eq!(calcs[1].name(), "BasicTireWearPMEmissionCalculator");
    }

    #[test]
    fn run_on_empty_inputs_yields_no_output() {
        assert!(
            BasicPm25Calculator::run(&BasicPm25Inputs::default(), &RunConstants::default())
                .is_empty()
        );
    }

    #[test]
    fn end_to_end_brakewear_single_flow() {
        let (inputs, constants) = brakewear_flow();
        let out = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        let row = out[0];
        assert_eq!(row.pollutant_id, BRAKEWEAR_POLLUTANT_ID);
        assert_eq!(row.process_id, 9);
        assert_eq!(row.year_id, 2020);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 8);
        assert_eq!(row.fuel_type_id, 1);
        assert_eq!(row.model_year_id, 2020);
        assert_eq!(row.state_id, 8);
        assert_eq!(row.county_id, 8001);
        assert_eq!(row.zone_id, 80010);
        assert_eq!(row.link_id, 800_101);
        assert_eq!(row.road_type_id, 5);
        // rate = 0.5 opModeFraction * 4.0 meanBaseRate = 2.0;
        // fully-weighted = 1.0 sourceBinActivityFraction * 2.0 = 2.0;
        // unadjusted = 2.0 * 10.0 SHO = 20.0;
        // fuel adjustment = 1.0 marketShare (no generalFuelRatio rows);
        // emissionQuant = 1.0 * 20.0 = 20.0.
        assert!((row.emission_quant - 20.0).abs() < 1e-9);
    }

    #[test]
    fn end_to_end_tirewear_single_flow() {
        // The tire-wear calculator drives the identical shared core; only the
        // polProcessID / (pollutant, process) differs.
        let (inputs, constants) = single_flow(TIREWEAR_POL_PROCESS_ID, TIREWEAR_POLLUTANT_ID, 10);
        let out = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, TIREWEAR_POLLUTANT_ID);
        assert_eq!(out[0].process_id, 10);
        assert!((out[0].emission_quant - 20.0).abs() < 1e-9);
    }

    #[test]
    fn weight_by_op_mode_computes_the_rate_product() {
        let (inputs, constants) = brakewear_flow();
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert_eq!(weighted.len(), 1);
        // 0.5 opModeFraction * 4.0 meanBaseRate.
        assert!((weighted[0].op_mode_weighted_mean_base_rate - 2.0).abs() < 1e-12);
        assert_eq!(weighted[0].source_bin_id, 100);
        assert_eq!(weighted[0].age_group_id, 3);
    }

    #[test]
    fn weight_by_op_mode_drops_rows_failing_the_existence_filter() {
        // Source type 99 has no SourceTypeModelYear row -> the existence
        // filter rejects every OpModeDistribution row for it.
        let (mut inputs, constants) = brakewear_flow();
        inputs.op_mode_distribution[0].source_type_id = 99;
        assert!(weight_by_op_mode(&inputs, &constants).is_empty());
    }

    #[test]
    fn weight_by_op_mode_sums_distinct_rates_across_operating_modes() {
        // A second operating mode with a *different* rate product: 0.25 * 8.0 =
        // 2.0 collapses onto the first (0.5 * 4.0 = 2.0) under DISTINCT, but a
        // distinct value sums. Use opMode 301 with 0.25 * 12.0 = 3.0.
        let (mut inputs, constants) = brakewear_flow();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 1,
            source_type_id: 21,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 301,
            op_mode_fraction: 0.25,
        });
        inputs.emission_rate.push(EmissionRateRow {
            source_bin_id: 100,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 301,
            mean_base_rate: 12.0,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert_eq!(weighted.len(), 1);
        // 2.0 (opMode 300) + 3.0 (opMode 301).
        assert!((weighted[0].op_mode_weighted_mean_base_rate - 5.0).abs() < 1e-12);
    }

    #[test]
    fn weight_by_op_mode_distinct_collapses_equal_rates() {
        // A second operating mode whose rate product is bit-identical to the
        // first (0.25 * 8.0 == 0.5 * 4.0 == 2.0). The temp table's SELECT
        // DISTINCT drops opModeID, so the two collapse to one row and the
        // GROUP BY ... SUM counts the rate ONCE, not twice.
        let (mut inputs, constants) = brakewear_flow();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 1,
            source_type_id: 21,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 301,
            op_mode_fraction: 0.25,
        });
        inputs.emission_rate.push(EmissionRateRow {
            source_bin_id: 100,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 301,
            mean_base_rate: 8.0,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert_eq!(weighted.len(), 1);
        assert!(
            (weighted[0].op_mode_weighted_mean_base_rate - 2.0).abs() < 1e-12,
            "equal rate products collapse under DISTINCT and sum once"
        );
    }

    #[test]
    fn weight_by_source_bin_sums_across_source_bins() {
        // A second source bin (200) for the same source type / age contributes
        // its own fully-weighted term.
        let (mut inputs, constants) = brakewear_flow();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 1,
            source_type_id: 21,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 300,
            op_mode_fraction: 0.5,
        });
        inputs.emission_rate.push(EmissionRateRow {
            source_bin_id: 200,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            op_mode_id: 300,
            mean_base_rate: 4.0,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 500,
                pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
                source_bin_id: 200,
                source_bin_activity_fraction: 0.5,
            });
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 200,
            fuel_type_id: 1,
            model_year_group_id: 7,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        let fully = weight_by_source_bin(&weighted, &inputs, &constants);
        assert_eq!(fully.len(), 1);
        // bin 100: 1.0 * 2.0 = 2.0; bin 200: 0.5 * 2.0 = 1.0.
        assert!((fully[0].fully_weighted_mean_base_rate - 3.0).abs() < 1e-12);
    }

    #[test]
    fn weight_by_source_bin_drops_mismatched_model_year_group() {
        // SourceBin's modelYearGroupID (99) no longer matches the
        // PollutantProcessModelYear group (7) -> the row is dropped.
        let (mut inputs, constants) = brakewear_flow();
        inputs.source_bin[0].model_year_group_id = 99;
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert!(weight_by_source_bin(&weighted, &inputs, &constants).is_empty());
    }

    #[test]
    fn multiply_by_activity_fans_out_per_sho_month() {
        let (mut inputs, constants) = brakewear_flow();
        // A second SHO month for the same source type / age / hour-day.
        inputs.sho.push(ShoRow {
            hour_day_id: 1,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            source_type_id: 21,
            sho: 4.0,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        let fully = weight_by_source_bin(&weighted, &inputs, &constants);
        let unadjusted = multiply_by_activity(&fully, &inputs);
        assert_eq!(unadjusted.len(), 2);
        let total: f64 = unadjusted.iter().map(|u| u.unadjusted_emission_quant).sum();
        // rate 2.0 * (10 + 4) SHO.
        assert!((total - 28.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_adjustment_uses_market_share_when_no_general_fuel_ratio() {
        // With no generalFuelRatio rows the LEFT JOIN null row gives a per
        // fuel-type adjustment equal to the summed marketShare.
        let (inputs, constants) = brakewear_flow();
        let fsft = build_fuel_supply_with_fuel_type(&inputs, &constants);
        let adjustment = build_fuel_supply_adjustment(&fsft, &inputs, &constants);
        let key = FuelAdjustmentKey {
            year_id: 2020,
            month_id: 7,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            model_year_id: 2020,
            source_type_id: 21,
            fuel_type_id: 1,
        };
        assert!((adjustment.get(&key).copied().unwrap_or(0.0) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_adjustment_applies_general_fuel_ratio() {
        // A generalFuelRatio row with fuelEffectRatio 1.5 scales the worker
        // output: emissionQuant = 1.5 * 20.0 = 30.0.
        let (mut inputs, constants) = brakewear_flow();
        inputs.general_fuel_ratio.push(GeneralFuelRatioRow {
            fuel_formulation_id: 9000,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            min_model_year_id: 1990,
            max_model_year_id: 2050,
            min_age_id: 0,
            max_age_id: 40,
            source_type_id: 21,
            fuel_effect_ratio: Some(1.5),
            fuel_effect_ratio_gpa: Some(1.5),
        });
        let out = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 30.0).abs() < 1e-9);
    }

    #[test]
    fn fuel_adjustment_blends_gpa_ratio_by_gpa_fraction() {
        // fer = 1.0, ferGPA = 2.0, GPAFract = 0.5 ->
        // 1.0 + 0.5 * (2.0 - 1.0) = 1.5; emissionQuant = 1.5 * 20.0 = 30.0.
        let (mut inputs, mut constants) = brakewear_flow();
        constants.gpa_fract = 0.5;
        inputs.general_fuel_ratio.push(GeneralFuelRatioRow {
            fuel_formulation_id: 9000,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            min_model_year_id: 1990,
            max_model_year_id: 2050,
            min_age_id: 0,
            max_age_id: 40,
            source_type_id: 21,
            fuel_effect_ratio: Some(1.0),
            fuel_effect_ratio_gpa: Some(2.0),
        });
        let out = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 30.0).abs() < 1e-9);
    }

    #[test]
    fn fuel_adjustment_treats_null_ratios_as_one() {
        // A matching generalFuelRatio row whose ratios are both NULL ->
        // ifnull(..., 1) makes the term reduce to marketShare, so the run is
        // unchanged from the no-generalFuelRatio case (emissionQuant 20.0).
        let (mut inputs, constants) = brakewear_flow();
        inputs.general_fuel_ratio.push(GeneralFuelRatioRow {
            fuel_formulation_id: 9000,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            min_model_year_id: 1990,
            max_model_year_id: 2050,
            min_age_id: 0,
            max_age_id: 40,
            source_type_id: 21,
            fuel_effect_ratio: None,
            fuel_effect_ratio_gpa: None,
        });
        let out = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 20.0).abs() < 1e-9);
    }

    #[test]
    fn fuel_supply_with_fuel_type_fans_a_month_group_to_its_months() {
        // The month group holds two months -> one FuelSupply row becomes two
        // FuelSupplyWithFuelType rows.
        let (mut inputs, constants) = brakewear_flow();
        inputs.month_of_any_year.push(MonthOfAnyYearRow {
            month_id: 8,
            month_group_id: 1,
        });
        let fsft = build_fuel_supply_with_fuel_type(&inputs, &constants);
        assert_eq!(fsft.len(), 2);
        let mut months: Vec<i32> = fsft.iter().map(|r| r.month_id).collect();
        months.sort_unstable();
        assert_eq!(months, vec![7, 8]);
    }

    #[test]
    fn fuel_adjusted_join_drops_unmatched_unadjusted_rows() {
        // An UnadjustedEmissionResults row with no FuelSupplyAdjustment match
        // is dropped by the inner join in step 4-c.
        let unadjusted = [UnadjustedEmission {
            year_id: 2020,
            month_id: 7,
            hour_day_id: 1,
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id: 2020,
            pol_process_id: BRAKEWEAR_POL_PROCESS_ID,
            unadjusted_emission_quant: 20.0,
        }];
        let empty: HashMap<FuelAdjustmentKey, f64> = HashMap::new();
        assert!(apply_fuel_adjustment(&unadjusted, &empty).is_empty());
    }

    #[test]
    fn run_is_deterministic_across_calls() {
        let (inputs, constants) = brakewear_flow();
        let first = BasicPm25Calculator::run(&inputs, &constants);
        let second = BasicPm25Calculator::run(&inputs, &constants);
        assert_eq!(first, second);
    }
}
