//! Port of `database/BasicStartPM25Calculator.sql` — the
//! `BasicStartPMEmissionCalculator`, MOVES's start-exhaust PM2.5 calculator.
//!
//! Migration plan: Phase 3, Task 54.
//!
//! # What this calculator does
//!
//! `BasicStartPMEmissionCalculator` computes start-exhaust PM2.5 emissions
//! for the two PM2.5 components MOVES tracks separately on the inventory
//! (non-rates) calculation path:
//!
//! * **Composite — Non-Elemental-Carbon PM2.5**, pollutant 118;
//! * **Elemental Carbon PM2.5**, pollutant 112,
//!
//! both for the **Start Exhaust** process (process 2). The two
//! `polProcessID`s the Java constructor names — `"11802"` and `"11202"` —
//! encode `pollutantID * 100 + processID`.
//!
//! # Supersession by `BaseRateCalculator`
//!
//! This is a **legacy** calculator that the modern rates-first
//! `BaseRateCalculator` (Task 45) superseded. The pinned MOVES runtime
//! registry `CalculatorInfo.txt` carries no `BasicStartPMEmissionCalculator`
//! entry, and `characterization/calculator-chains/calculator-dag.json`
//! records `registrations_count: 0` for it; the `(112, 2)` and `(118, 2)`
//! start-exhaust PM2.5 pairs are registered to `BaseRateCalculator` instead.
//! [`Calculator::registrations`] therefore returns an **empty slice** —
//! re-registering the pairs here would collide with the already-merged
//! `BaseRateCalculator`. The compute core is still ported in full (it is the
//! task's real deliverable), and [`Calculator::subscriptions`] still mirrors
//! the Java `subscribeToMe`.
//!
//! # Java / SQL structure
//!
//! Unlike the rates-path calculators (e.g. `BaseRateCalculator`), this is a
//! legacy inventory-path calculator with **no Go worker**: the Java class
//! `BasicStartPMEmissionCalculator` is a thin `GenericCalculatorBase`
//! subclass that names a SQL script, and the script
//! `database/BasicStartPM25Calculator.sql` carries the whole computation.
//! This module ports that script's `Section Processing`; the script's
//! `Section Extract Data` is the data-plane table load that Task 50
//! (`DataFrameStore`) materialises.
//!
//! # The five-step pipeline
//!
//! 1. **Weight emission rates by operating mode** (`weight_by_op_mode`) —
//!    `opModeFraction * meanBaseRate`, joined `OpModeDistribution` ⋈
//!    `EmissionRateByAge` and gated by the
//!    `SourceBinDistribution`/`AgeCategory`/`SourceTypeModelYear` existence
//!    chain. Produces the `OpModeWeightedEmissionRate` rows.
//! 2. **Weight by source bin** (`weight_by_source_bin`) — sums
//!    `sourceBinActivityFraction * opModeWeightedMeanBaseRate` across source
//!    bins and operating modes, resolving the fuel type through `SourceBin`.
//!    Produces the `FullyWeightedEmissionRate` rows.
//! 3. **Multiply by activity** (`multiply_by_activity`) — multiplies the
//!    fully-weighted rate by the `Starts` count. Produces the
//!    `UnadjustedEmissionResults` rows.
//! 4. **Apply temperature adjustment** (`apply_temperature_adjustment`) —
//!    applies the `StartTempAdjustment` exponential temperature correction.
//!    Produces the `AdjustedEmissionResults` rows.
//! 5. **Convert to worker output** (`to_worker_output`) — splits
//!    `polProcessID` into `(pollutant, process)`, attaches the location
//!    columns, and applies the `generalFuelRatio` fuel-effect multiplier.
//!    Produces [`WorkerOutputRow`]s.
//!
//! [`BasicStartPmEmissionCalculator::run`] chains all five.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` intermediate columns.** The SQL holds every intermediate
//!   rate (`opModeWeightedMeanBaseRate`, `fullyWeightedMeanBaseRate`,
//!   `unadjustedEmissionQuant`, `emissionQuant`) in 32-bit `FLOAT` temp
//!   columns, and the `meanBaseRate` / `opModeFraction` /
//!   `sourceBinActivityFraction` / `starts` / `tempAdjustTerm*` source
//!   columns are `FLOAT` too. MariaDB evaluates the arithmetic in `DOUBLE`
//!   but truncates to `f32` at each temp-table write. This port computes in
//!   `f64` throughout; per the Task 33 / Task 41 precedent the
//!   bug-compatibility decision is deferred to Task 44.
//! * **Loose `GROUP BY` in step 2.** The `FullyWeightedEmissionRate` insert
//!   groups by everything *except* `opModeID` yet the `SELECT` still
//!   projects `omer.opModeID`; MariaDB returns the value from an
//!   indeterminate row of each group. That `opModeID` then selects the
//!   `StartTempAdjustment` row in step 4, so the choice is observable. This
//!   port picks the **minimum** `opModeID` of each group as a deterministic,
//!   reproducible representative — flagged here for Task 44.
//! * **No division.** The processing pipeline contains no integer division,
//!   so the MariaDB `int / int` rounding gotcha does not arise.
//!
//! # Road-type gate
//!
//! Start-exhaust emissions are reported only on the off-network road type
//! (`roadTypeID = 1`): the Java `doExecute` returns no SQL for a Start
//! Exhaust context whose `roadTypeRecordID != 1`, and `doesProcessContext`
//! rejects any positive non-1 road type. [`BasicStartPmEmissionCalculator::processes_road_type`]
//! ports that gate; the master loop (Task 50) consults it before invoking
//! the calculator.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders, so it
//! cannot yet materialise a [`BasicStartPmInputs`] nor write the worker
//! output back. The numerically faithful pipeline is fully ported and
//! unit-tested on [`BasicStartPmEmissionCalculator::run`]; once the
//! `DataFrameStore` lands, `execute` builds the inputs from `ctx.tables()`,
//! calls `run`, and stores the [`WorkerOutputRow`]s.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// MOVES process id for Start Exhaust — the single process the calculator's
/// `polProcessID`s (`11802`, `11202`) span.
const START_EXHAUST_PROCESS_ID: u16 = 2;
/// The off-network road type — the only road type start exhaust runs on.
const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

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
    /// `opModeID` — operating mode (a soak-time bin for start exhaust).
    pub op_mode_id: i32,
    /// `opModeFraction` — fraction of activity in this operating mode.
    pub op_mode_fraction: f64,
}

/// One `EmissionRateByAge` row — the per-operating-mode mean base rate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `sourceBinID` — source bin (a `BIGINT` in MOVES).
    pub source_bin_id: i64,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `ageGroupID` — the vehicle age group this rate applies to.
    pub age_group_id: i32,
    /// `meanBaseRate` — the mean base emission rate.
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

/// One `Starts` row — the start-event count for a source type / time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
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
    /// `starts` — the number of start events.
    pub starts: f64,
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

/// One `ZoneMonthHour` row — the temperature for a month / hour.
///
/// The extract filters to the iteration zone, so `zoneID` is constant and
/// not modeled. `temperature` is a nullable `DOUBLE`; a `None` propagates a
/// SQL `NULL` through `least(...)` and disables the temperature adjustment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `monthID`.
    pub month_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `temperature` — ambient temperature in °F, or `None` for SQL `NULL`.
    pub temperature: Option<f64>,
}

/// One `PollutantProcessMappedModelYear` row — maps `(polProcessID,
/// modelYearID)` to the model-year group used for temperature adjustment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessMappedModelYearRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
}

/// One `StartTempAdjustment` row — the exponential temperature-correction
/// terms for one `(polProcessID, fuelTypeID, modelYearGroupID, opModeID)`.
///
/// Each `tempAdjustTerm*` is a nullable `FLOAT`; a `None` in any of the three
/// makes the whole correction expression `NULL`, so the SQL `coalesce`
/// falls back to the unadjusted quantity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartTempAdjustmentRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearGroupID`.
    pub model_year_group_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `tempAdjustTermA` — the exponential-rate term.
    pub temp_adjust_term_a: Option<f64>,
    /// `tempAdjustTermB` — the multiplicative term.
    pub temp_adjust_term_b: Option<f64>,
    /// `tempAdjustTermC` — the additive term.
    pub temp_adjust_term_c: Option<f64>,
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

/// One `oneCountyYearGeneralFuelRatio` row — the fuel-effect multiplier the
/// SQL builds in `Section Extract Data` and applies to the worker output.
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
    /// `fuelEffectRatio` — the multiplier applied to `emissionQuant`.
    pub fuel_effect_ratio: f64,
}

/// The fully materialised set of input tables one calculator run consumes.
///
/// Mirrors the tables `BasicStartPM25Calculator.sql`'s `Section Processing`
/// reads. The data plane (Task 50) builds this from the filtered execution
/// database; tests build it directly.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicStartPmInputs {
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
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
    /// `Starts` rows.
    pub starts: Vec<StartsRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `ZoneMonthHour` rows.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
    /// `PollutantProcessMappedModelYear` rows.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `StartTempAdjustment` rows.
    pub start_temp_adjustment: Vec<StartTempAdjustmentRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `oneCountyYearGeneralFuelRatio` rows.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
}

/// The scalar run constants the SQL reads from `##context...##` macros.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    op_mode_id: i32,
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
    op_mode_id: i32,
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
    op_mode_id: i32,
    unadjusted_emission_quant: f64,
}

/// Step 4 output — `AdjustedEmissionResults`.
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
    /// `emissionQuant` — the emission quantity, fuel-effect adjusted.
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
    op_mode_id: i32,
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

/// Step 2 accumulator value — the running sum plus the `min`-representative
/// operating mode (see the loose-`GROUP BY` fidelity note in the module docs).
#[derive(Debug, Clone, Copy)]
struct FullyWeightedAcc {
    sum: f64,
    op_mode_id: i32,
}

/// `GROUP BY` key of step 4's `AdjustedEmissionResults`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AdjustedKey {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    pol_process_id: i32,
}

/// Join key of the `oneCountyYearGeneralFuelRatio` fuel-effect lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct GeneralFuelRatioKey {
    fuel_type_id: i32,
    source_type_id: i32,
    month_id: i32,
    pollutant_id: i32,
    process_id: i32,
    model_year_id: i32,
    year_id: i32,
}

// ===========================================================================
// Step 1 — weight emission rates by operating mode.
// ===========================================================================

/// Step 1 — `OpModeWeightedEmissionRate`.
///
/// Ports the `OpModeWeightedEmissionRateTemp` insert plus the
/// `OpModeWeightedEmissionRate` `GROUP BY` that follows it. The temp insert's
/// `SELECT DISTINCT` projects only `OpModeDistribution` and
/// `EmissionRateByAge` columns — the `SourceBinDistribution`, `AgeCategory`
/// and `SourceTypeModelYear` joins contribute *no* projected columns, so they
/// act purely as an existence filter and the `DISTINCT` collapses their join
/// multiplicity. Because `opModeFraction * meanBaseRate` is a function of the
/// six-column key, the subsequent `GROUP BY ... SUM` reduces each group to
/// that single value — modeled here as a keyed map insert.
fn weight_by_op_mode(
    inputs: &BasicStartPmInputs,
    constants: &RunConstants,
) -> Vec<OpModeWeightedRate> {
    // EmissionRateByAge indexed by the `USING (polProcessID, opModeID)` join.
    let mut er_by_pol_op: HashMap<(i32, i32), Vec<&EmissionRateByAgeRow>> = HashMap::new();
    for er in &inputs.emission_rate_by_age {
        er_by_pol_op
            .entry((er.pol_process_id, er.op_mode_id))
            .or_default()
            .push(er);
    }
    // AgeCategory age ids grouped by age group.
    let mut age_ids_by_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for acat in &inputs.age_category {
        age_ids_by_group
            .entry(acat.age_group_id)
            .or_default()
            .push(acat.age_id);
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

    let mut acc: BTreeMap<OpModeWeightedKey, f64> = BTreeMap::new();
    for omd in &inputs.op_mode_distribution {
        let Some(ers) = er_by_pol_op.get(&(omd.pol_process_id, omd.op_mode_id)) else {
            continue;
        };
        for er in ers {
            let Some(age_ids) = age_ids_by_group.get(&er.age_group_id) else {
                continue;
            };
            let Some(stmy_ids) = stmy_ids_by_pol_bin.get(&(er.pol_process_id, er.source_bin_id))
            else {
                continue;
            };
            // The existence filter: some age in the group must reach a
            // `SourceTypeModelYear` row whose source type matches the
            // `OpModeDistribution` row and whose model year is `year - ageID`.
            let exists = age_ids.iter().any(|&age_id| {
                let target_model_year = constants.year - age_id;
                stmy_ids.iter().any(|&stmy_id| {
                    stmy_by_id.get(&stmy_id).is_some_and(|stmy| {
                        stmy.source_type_id == omd.source_type_id
                            && stmy.model_year_id == target_model_year
                    })
                })
            });
            if !exists {
                continue;
            }
            let key = OpModeWeightedKey {
                hour_day_id: omd.hour_day_id,
                source_type_id: omd.source_type_id,
                source_bin_id: er.source_bin_id,
                age_group_id: er.age_group_id,
                pol_process_id: omd.pol_process_id,
                op_mode_id: omd.op_mode_id,
            };
            // The post-`DISTINCT` group holds one row, so the insert *is* the
            // `SUM` (a duplicate omd/er row would re-insert the same value).
            acc.insert(key, omd.op_mode_fraction * er.mean_base_rate);
        }
    }
    acc.into_iter()
        .map(|(key, rate)| OpModeWeightedRate {
            hour_day_id: key.hour_day_id,
            source_type_id: key.source_type_id,
            source_bin_id: key.source_bin_id,
            age_group_id: key.age_group_id,
            pol_process_id: key.pol_process_id,
            op_mode_id: key.op_mode_id,
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
/// `SourceBinDistribution` ⋈ `AgeCategory` ⋈ `SourceTypeModelYear` ⋈
/// `PollutantProcessModelYear` ⋈ `SourceBin` join, grouped by everything but
/// the operating mode. The non-grouped `omer.opModeID` the `SELECT` projects
/// is indeterminate in MariaDB; this port keeps the `min` of each group (see
/// the module-level fidelity note).
fn weight_by_source_bin(
    op_mode_weighted: &[OpModeWeightedRate],
    inputs: &BasicStartPmInputs,
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
    // AgeCategory: `ageID -> ageGroupID` (the `ageID` is the `AgeCategory` key).
    let age_group_of: HashMap<i32, i32> = inputs
        .age_category
        .iter()
        .map(|acat| (acat.age_id, acat.age_group_id))
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

    let mut acc: BTreeMap<FullyWeightedKey, FullyWeightedAcc> = BTreeMap::new();
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
            if age_group_of.get(&age_id) != Some(&omer.age_group_id) {
                continue;
            }
            let Some(ppmy) = ppmy_by.get(&(sbd.pol_process_id, stmy.model_year_id)) else {
                continue;
            };
            let Some(sb) = sb_by_id.get(&sbd.source_bin_id) else {
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
            let contribution =
                sbd.source_bin_activity_fraction * omer.op_mode_weighted_mean_base_rate;
            acc.entry(key)
                .and_modify(|a| {
                    a.sum += contribution;
                    a.op_mode_id = a.op_mode_id.min(omer.op_mode_id);
                })
                .or_insert(FullyWeightedAcc {
                    sum: contribution,
                    op_mode_id: omer.op_mode_id,
                });
        }
    }
    acc.into_iter()
        .map(|(key, a)| FullyWeightedRate {
            year_id: key.year_id,
            hour_day_id: key.hour_day_id,
            source_type_id: key.source_type_id,
            fuel_type_id: key.fuel_type_id,
            model_year_id: key.model_year_id,
            pol_process_id: key.pol_process_id,
            fully_weighted_mean_base_rate: a.sum,
            age_id: key.age_id,
            op_mode_id: a.op_mode_id,
        })
        .collect()
}

// ===========================================================================
// Step 3 — multiply emission rates by activity.
// ===========================================================================

/// Step 3 — `UnadjustedEmissionResults`.
///
/// Multiplies each fully-weighted rate by the matching `Starts` count, joined
/// `USING (hourDayID, yearID, ageID, sourceTypeID)`. A rate with several
/// start months produces one unadjusted-emission row per month.
fn multiply_by_activity(
    fully_weighted: &[FullyWeightedRate],
    inputs: &BasicStartPmInputs,
) -> Vec<UnadjustedEmission> {
    let mut starts_by: HashMap<(i32, i32, i32, i32), Vec<&StartsRow>> = HashMap::new();
    for s in &inputs.starts {
        starts_by
            .entry((s.hour_day_id, s.year_id, s.age_id, s.source_type_id))
            .or_default()
            .push(s);
    }

    let mut out = Vec::new();
    for fwer in fully_weighted {
        let Some(starts_rows) = starts_by.get(&(
            fwer.hour_day_id,
            fwer.year_id,
            fwer.age_id,
            fwer.source_type_id,
        )) else {
            continue;
        };
        for s in starts_rows {
            out.push(UnadjustedEmission {
                year_id: fwer.year_id,
                month_id: s.month_id,
                hour_day_id: fwer.hour_day_id,
                source_type_id: fwer.source_type_id,
                fuel_type_id: fwer.fuel_type_id,
                model_year_id: fwer.model_year_id,
                pol_process_id: fwer.pol_process_id,
                op_mode_id: fwer.op_mode_id,
                unadjusted_emission_quant: fwer.fully_weighted_mean_base_rate * s.starts,
            });
        }
    }
    out
}

// ===========================================================================
// Step 4 — apply temperature adjustment.
// ===========================================================================

/// The SQL `coalesce(unadjustedEmissionQuant * tempAdjustTermB *
/// exp(tempAdjustTermA * (72.0 - least(temperature, 72))) + tempAdjustTermC,
/// unadjustedEmissionQuant)`.
///
/// Any `NULL` operand — a missing `StartTempAdjustment` row, a `NULL`
/// adjustment term, or a `NULL` temperature — makes the correction
/// expression `NULL`, so the result falls back to the unadjusted quantity.
fn adjusted_quant(
    unadjusted: f64,
    temperature: Option<f64>,
    adjustment: Option<&StartTempAdjustmentRow>,
) -> f64 {
    let corrected = (|| {
        let ta = adjustment?;
        let a = ta.temp_adjust_term_a?;
        let b = ta.temp_adjust_term_b?;
        let c = ta.temp_adjust_term_c?;
        let temp = temperature?;
        Some(unadjusted * b * (a * (72.0 - temp.min(72.0))).exp() + c)
    })();
    corrected.unwrap_or(unadjusted)
}

/// Step 4 — `AdjustedEmissionResults`.
///
/// Decodes `hourDayID` into `(dayID, hourID)`, looks up the zone/month/hour
/// temperature, resolves the model-year group through
/// `PollutantProcessMappedModelYear`, applies the `StartTempAdjustment` left
/// join via [`adjusted_quant`], and sums by the day/hour-keyed group.
fn apply_temperature_adjustment(
    unadjusted: &[UnadjustedEmission],
    inputs: &BasicStartPmInputs,
) -> Vec<AdjustedEmission> {
    let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let temp_by_month_hour: HashMap<(i32, i32), Option<f64>> = inputs
        .zone_month_hour
        .iter()
        .map(|zmh| ((zmh.month_id, zmh.hour_id), zmh.temperature))
        .collect();
    let ppmmy_group_by: HashMap<(i32, i32), i32> = inputs
        .pollutant_process_mapped_model_year
        .iter()
        .map(|ppmmy| {
            (
                (ppmmy.pol_process_id, ppmmy.model_year_id),
                ppmmy.model_year_group_id,
            )
        })
        .collect();
    let sta_by: HashMap<(i32, i32, i32, i32), &StartTempAdjustmentRow> = inputs
        .start_temp_adjustment
        .iter()
        .map(|ta| {
            (
                (
                    ta.pol_process_id,
                    ta.fuel_type_id,
                    ta.op_mode_id,
                    ta.model_year_group_id,
                ),
                ta,
            )
        })
        .collect();

    let mut acc: BTreeMap<AdjustedKey, f64> = BTreeMap::new();
    for uer in unadjusted {
        let Some(hd) = hour_day_by_id.get(&uer.hour_day_id) else {
            continue;
        };
        let Some(&temperature) = temp_by_month_hour.get(&(uer.month_id, hd.hour_id)) else {
            continue;
        };
        let Some(&model_year_group_id) =
            ppmmy_group_by.get(&(uer.pol_process_id, uer.model_year_id))
        else {
            continue;
        };
        let adjustment = sta_by
            .get(&(
                uer.pol_process_id,
                uer.fuel_type_id,
                uer.op_mode_id,
                model_year_group_id,
            ))
            .copied();
        let emission = adjusted_quant(uer.unadjusted_emission_quant, temperature, adjustment);
        let key = AdjustedKey {
            year_id: uer.year_id,
            month_id: uer.month_id,
            day_id: hd.day_id,
            hour_id: hd.hour_id,
            source_type_id: uer.source_type_id,
            fuel_type_id: uer.fuel_type_id,
            model_year_id: uer.model_year_id,
            pol_process_id: uer.pol_process_id,
        };
        *acc.entry(key).or_insert(0.0) += emission;
    }
    acc.into_iter()
        .map(|(key, emission_quant)| AdjustedEmission {
            year_id: key.year_id,
            month_id: key.month_id,
            day_id: key.day_id,
            hour_id: key.hour_id,
            source_type_id: key.source_type_id,
            fuel_type_id: key.fuel_type_id,
            model_year_id: key.model_year_id,
            pol_process_id: key.pol_process_id,
            emission_quant,
        })
        .collect()
}

// ===========================================================================
// Step 5 — convert to MOVESWorkerOutput.
// ===========================================================================

/// Step 5 — the `MOVESWorkerOutput` insert plus the `oneCountyYearGeneralFuelRatio`
/// fuel-effect update.
///
/// Splits `polProcessID` into `(pollutantID, processID)` through
/// `PollutantProcessAssoc`, attaches the constant location columns, and
/// multiplies `emissionQuant` by the matching `fuelEffectRatio`.
fn to_worker_output(
    adjusted: &[AdjustedEmission],
    inputs: &BasicStartPmInputs,
    constants: &RunConstants,
) -> Vec<WorkerOutputRow> {
    let ppa_by: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|ppa| (ppa.pol_process_id, ppa))
        .collect();
    let fuel_ratio_by: HashMap<GeneralFuelRatioKey, f64> = inputs
        .general_fuel_ratio
        .iter()
        .map(|gfr| {
            (
                GeneralFuelRatioKey {
                    fuel_type_id: gfr.fuel_type_id,
                    source_type_id: gfr.source_type_id,
                    month_id: gfr.month_id,
                    pollutant_id: gfr.pollutant_id,
                    process_id: gfr.process_id,
                    model_year_id: gfr.model_year_id,
                    year_id: gfr.year_id,
                },
                gfr.fuel_effect_ratio,
            )
        })
        .collect();

    let mut out = Vec::new();
    for aer in adjusted {
        let Some(ppa) = ppa_by.get(&aer.pol_process_id) else {
            continue;
        };
        let mut row = WorkerOutputRow {
            year_id: aer.year_id,
            month_id: aer.month_id,
            day_id: aer.day_id,
            hour_id: aer.hour_id,
            state_id: constants.state_id,
            county_id: constants.county_id,
            zone_id: constants.zone_id,
            link_id: constants.link_id,
            pollutant_id: ppa.pollutant_id,
            process_id: ppa.process_id,
            source_type_id: aer.source_type_id,
            fuel_type_id: aer.fuel_type_id,
            model_year_id: aer.model_year_id,
            road_type_id: constants.road_type_id,
            emission_quant: aer.emission_quant,
        };
        if let Some(&ratio) = fuel_ratio_by.get(&GeneralFuelRatioKey {
            fuel_type_id: row.fuel_type_id,
            source_type_id: row.source_type_id,
            month_id: row.month_id,
            pollutant_id: row.pollutant_id,
            process_id: row.process_id,
            model_year_id: row.model_year_id,
            year_id: row.year_id,
        }) {
            row.emission_quant *= ratio;
        }
        out.push(row);
    }
    out
}

// ===========================================================================
// The calculator.
// ===========================================================================

/// The Basic Start PM Emission Calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait requires. All run-varying input flows through
/// [`BasicStartPmEmissionCalculator::run`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicStartPmEmissionCalculator;

impl BasicStartPmEmissionCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = "BasicStartPMEmissionCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Whether start exhaust is reported on the given road type.
    ///
    /// Ports the Java `doExecute` / `doesProcessContext` road-type gate:
    /// start-exhaust emissions live only on the off-network road type
    /// (`roadTypeID = 1`).
    #[must_use]
    pub fn processes_road_type(road_type_id: i32) -> bool {
        road_type_id == OFF_NETWORK_ROAD_TYPE_ID
    }

    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Chains the five processing steps of `BasicStartPM25Calculator.sql` and
    /// returns the `MOVESWorkerOutput` rows the SQL would insert. Output rows
    /// are ordered deterministically by the steps' `BTreeMap` accumulators.
    #[must_use]
    pub fn run(inputs: &BasicStartPmInputs, constants: &RunConstants) -> Vec<WorkerOutputRow> {
        let op_mode_weighted = weight_by_op_mode(inputs, constants);
        let fully_weighted = weight_by_source_bin(&op_mode_weighted, inputs, constants);
        let unadjusted = multiply_by_activity(&fully_weighted, inputs);
        let adjusted = apply_temperature_adjustment(&unadjusted, inputs);
        to_worker_output(&adjusted, inputs, constants)
    }
}

/// The calculator's master-loop subscription.
///
/// `GenericCalculatorBase.subscribeToMe` subscribes once per process the
/// calculator's `polProcessID`s span; both `11802` and `11202` are process 2
/// (Start Exhaust), so there is exactly one subscription, at `YEAR`
/// granularity and `EMISSION_CALCULATOR` priority (the Java constructor
/// passes a zero priority offset). `calculator-dag.json` records the
/// granularity and priority but a placeholder `process_id` of 0, because the
/// static analyser cannot resolve `GenericCalculatorBase`'s runtime
/// `polProcessID` lookup — the true process id (2) comes from the constructor.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("EMISSION_CALCULATOR is a valid priority");
        [CalculatorSubscription::new(
            ProcessId(START_EXHAUST_PROCESS_ID),
            Granularity::Year,
            priority,
        )]
    })
}

/// The `(pollutant, process)` pairs the calculator registers — **none**.
///
/// `BasicStartPMEmissionCalculator` is superseded by `BaseRateCalculator`
/// (see the module-level supersession note): it is absent from
/// `CalculatorInfo.txt` and `calculator-dag.json` records
/// `registrations_count: 0`. Its start-exhaust PM2.5 pairs `(112, 2)` and
/// `(118, 2)` are registered to `BaseRateCalculator`, so registering them
/// here too would double-register them in the calculator registry. The Java
/// constructor's legacy `EmissionCalculatorRegistration.register(...)` calls
/// are intentionally not ported.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables `BasicStartPM25Calculator.sql`'s processing pass reads.
///
/// The script's extract section also pulls `County`, `Zone`, `Pollutant`,
/// `EmissionProcess`, `ModelYearGroup` and `TemperatureAdjustment`, which the
/// processing pass does not consume; they are omitted here. `generalFuelRatio`
/// is the default-DB source of the `oneCountyYearGeneralFuelRatio` scratch
/// table the fuel-effect step applies.
static INPUT_TABLES: &[&str] = &[
    "OpModeDistribution",
    "EmissionRateByAge",
    "SourceBinDistribution",
    "AgeCategory",
    "SourceTypeModelYear",
    "PollutantProcessModelYear",
    "SourceBin",
    "Starts",
    "HourDay",
    "ZoneMonthHour",
    "PollutantProcessMappedModelYear",
    "StartTempAdjustment",
    "PollutantProcessAssoc",
    "Link",
    "generalFuelRatio",
];

impl Calculator for BasicStartPmEmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Run the calculator for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace`, so this body
    /// cannot build a [`BasicStartPmInputs`] nor write the worker output
    /// back. The faithful pipeline is ported and tested on
    /// [`BasicStartPmEmissionCalculator::run`]; once the `DataFrameStore`
    /// lands, `execute` materialises the inputs from `ctx.tables()`, calls
    /// `run`, and stores the [`WorkerOutputRow`]s.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `polProcessID` for Composite Non-EC PM2.5, Start Exhaust (Java `"11802"`).
    const NON_EC_PM_START_POL_PROCESS_ID: i32 = 11802;
    /// `polProcessID` for Elemental Carbon PM2.5, Start Exhaust (Java `"11202"`).
    const EC_PM_START_POL_PROCESS_ID: i32 = 11202;
    /// MOVES pollutant id for Composite — Non-EC PM2.5.
    const NON_EC_PM_POLLUTANT_ID: u16 = 118;
    /// MOVES pollutant id for Elemental Carbon PM2.5.
    const EC_PM_POLLUTANT_ID: u16 = 112;

    /// A minimal set of inputs that flows one emission cleanly through all
    /// five steps. `year = 2020`, one source type, one source bin, age 0.
    fn single_flow_inputs() -> (BasicStartPmInputs, RunConstants) {
        let inputs = BasicStartPmInputs {
            op_mode_distribution: vec![OpModeDistributionRow {
                hour_day_id: 1,
                source_type_id: 21,
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                op_mode_id: 101,
                op_mode_fraction: 0.5,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 100,
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                op_mode_id: 101,
                age_group_id: 3,
                mean_base_rate: 2.0,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 500,
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
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
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                model_year_id: 2020,
                model_year_group_id: 7,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 100,
                fuel_type_id: 1,
                model_year_group_id: 7,
            }],
            starts: vec![StartsRow {
                hour_day_id: 1,
                month_id: 7,
                year_id: 2020,
                age_id: 0,
                source_type_id: 21,
                starts: 10.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 1,
                day_id: 5,
                hour_id: 8,
            }],
            zone_month_hour: vec![ZoneMonthHourRow {
                month_id: 7,
                hour_id: 8,
                temperature: Some(50.0),
            }],
            pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                model_year_id: 2020,
                model_year_group_id: 7,
            }],
            start_temp_adjustment: vec![StartTempAdjustmentRow {
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                fuel_type_id: 1,
                model_year_group_id: 7,
                op_mode_id: 101,
                temp_adjust_term_a: Some(0.04),
                temp_adjust_term_b: Some(1.0),
                temp_adjust_term_c: Some(0.0),
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
                pollutant_id: i32::from(NON_EC_PM_POLLUTANT_ID),
                process_id: i32::from(START_EXHAUST_PROCESS_ID),
            }],
            general_fuel_ratio: Vec::new(),
        };
        let constants = RunConstants {
            year: 2020,
            state_id: 8,
            county_id: 8001,
            zone_id: 80010,
            link_id: 800_101,
            road_type_id: 1,
        };
        (inputs, constants)
    }

    #[test]
    fn metadata_matches_the_dag_entry() {
        let calc = BasicStartPmEmissionCalculator::new();
        assert_eq!(calc.name(), "BasicStartPMEmissionCalculator");

        // One subscription: Start Exhaust, YEAR, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(2));
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

        // No registrations — superseded by BaseRateCalculator, which owns the
        // (112, 2) / (118, 2) pairs (calculator-dag.json: registrations_count 0).
        assert!(calc.registrations().is_empty());

        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().contains(&"StartTempAdjustment"));
        assert!(calc.input_tables().contains(&"Starts"));
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let calc = BasicStartPmEmissionCalculator::new();
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_ok());
    }

    #[test]
    fn calculator_is_object_safe() {
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(BasicStartPmEmissionCalculator::new())];
        assert_eq!(calcs[0].name(), "BasicStartPMEmissionCalculator");
    }

    #[test]
    fn road_type_gate_admits_only_off_network() {
        assert!(BasicStartPmEmissionCalculator::processes_road_type(1));
        assert!(!BasicStartPmEmissionCalculator::processes_road_type(2));
        assert!(!BasicStartPmEmissionCalculator::processes_road_type(5));
    }

    #[test]
    fn run_on_empty_inputs_yields_no_output() {
        assert!(BasicStartPmEmissionCalculator::run(
            &BasicStartPmInputs::default(),
            &RunConstants::default()
        )
        .is_empty());
    }

    #[test]
    fn end_to_end_single_flow() {
        let (inputs, constants) = single_flow_inputs();
        let out = BasicStartPmEmissionCalculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        let row = out[0];
        assert_eq!(row.pollutant_id, 118);
        assert_eq!(row.process_id, 2);
        assert_eq!(row.year_id, 2020);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 8);
        assert_eq!(row.fuel_type_id, 1);
        assert_eq!(row.model_year_id, 2020);
        assert_eq!(row.state_id, 8);
        assert_eq!(row.road_type_id, 1);
        // rate = 0.5 * 2.0 = 1.0; quant = 1.0 * 10 starts = 10.0;
        // adjusted = 10 * B * exp(A * (72 - min(50, 72))) + C
        //          = 10 * 1.0 * exp(0.04 * 22) + 0.0
        let expected = 10.0 * (0.04_f64 * 22.0).exp();
        assert!((row.emission_quant - expected).abs() < 1e-9);
    }

    #[test]
    fn weight_by_op_mode_drops_rows_failing_the_existence_filter() {
        // Source type 99 has no SourceTypeModelYear row -> the existence
        // filter rejects every OpModeDistribution row for it.
        let (mut inputs, constants) = single_flow_inputs();
        inputs.op_mode_distribution[0].source_type_id = 99;
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert!(weighted.is_empty());
    }

    #[test]
    fn weight_by_op_mode_computes_the_rate_product() {
        let (inputs, constants) = single_flow_inputs();
        let weighted = weight_by_op_mode(&inputs, &constants);
        assert_eq!(weighted.len(), 1);
        // 0.5 opModeFraction * 2.0 meanBaseRate.
        assert!((weighted[0].op_mode_weighted_mean_base_rate - 1.0).abs() < 1e-12);
        assert_eq!(weighted[0].op_mode_id, 101);
        assert_eq!(weighted[0].source_bin_id, 100);
    }

    #[test]
    fn weight_by_source_bin_keeps_the_min_op_mode_representative() {
        // Two operating modes in the same fully-weighted group: the loose
        // GROUP BY representative is the minimum opModeID, and the rate sums.
        let (mut inputs, constants) = single_flow_inputs();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 1,
            source_type_id: 21,
            pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
            op_mode_id: 105,
            op_mode_fraction: 0.25,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 100,
            pol_process_id: NON_EC_PM_START_POL_PROCESS_ID,
            op_mode_id: 105,
            age_group_id: 3,
            mean_base_rate: 2.0,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        let fully = weight_by_source_bin(&weighted, &inputs, &constants);
        assert_eq!(fully.len(), 1);
        // opMode 101: 0.5 * 2.0 = 1.0; opMode 105: 0.25 * 2.0 = 0.5.
        assert!((fully[0].fully_weighted_mean_base_rate - 1.5).abs() < 1e-12);
        assert_eq!(
            fully[0].op_mode_id, 101,
            "min opModeID is the representative"
        );
    }

    #[test]
    fn multiply_by_activity_fans_out_per_start_month() {
        let (mut inputs, constants) = single_flow_inputs();
        // A second start month for the same source type / age / hour-day.
        inputs.starts.push(StartsRow {
            hour_day_id: 1,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            source_type_id: 21,
            starts: 4.0,
        });
        let weighted = weight_by_op_mode(&inputs, &constants);
        let fully = weight_by_source_bin(&weighted, &inputs, &constants);
        let unadjusted = multiply_by_activity(&fully, &inputs);
        assert_eq!(unadjusted.len(), 2);
        let total: f64 = unadjusted.iter().map(|u| u.unadjusted_emission_quant).sum();
        // rate 1.0 * (10 + 4) starts.
        assert!((total - 14.0).abs() < 1e-12);
    }

    #[test]
    fn temperature_adjustment_falls_back_when_a_term_is_null() {
        // tempAdjustTermC NULL -> the correction expression is NULL ->
        // coalesce yields the unadjusted quantity.
        let null_c = StartTempAdjustmentRow {
            pol_process_id: 1,
            fuel_type_id: 1,
            model_year_group_id: 1,
            op_mode_id: 1,
            temp_adjust_term_a: Some(0.04),
            temp_adjust_term_b: Some(1.0),
            temp_adjust_term_c: None,
        };
        assert!((adjusted_quant(7.5, Some(40.0), Some(&null_c)) - 7.5).abs() < 1e-12);
        // No StartTempAdjustment row at all -> also unadjusted.
        assert!((adjusted_quant(7.5, Some(40.0), None) - 7.5).abs() < 1e-12);
        // NULL temperature -> unadjusted.
        let full = StartTempAdjustmentRow {
            temp_adjust_term_c: Some(1.0),
            ..null_c
        };
        assert!((adjusted_quant(7.5, None, Some(&full)) - 7.5).abs() < 1e-12);
    }

    #[test]
    fn temperature_adjustment_caps_temperature_at_72() {
        let ta = StartTempAdjustmentRow {
            pol_process_id: 1,
            fuel_type_id: 1,
            model_year_group_id: 1,
            op_mode_id: 1,
            temp_adjust_term_a: Some(0.1),
            temp_adjust_term_b: Some(1.0),
            temp_adjust_term_c: Some(0.0),
        };
        // At 80 °F (> 72) the `least(temperature, 72)` cap drives the
        // exponent to zero, so the result is just `quant * B + C`.
        assert!((adjusted_quant(3.0, Some(80.0), Some(&ta)) - 3.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_effect_ratio_scales_the_worker_output() {
        let (mut inputs, constants) = single_flow_inputs();
        inputs.general_fuel_ratio.push(GeneralFuelRatioRow {
            fuel_type_id: 1,
            source_type_id: 21,
            month_id: 7,
            pollutant_id: i32::from(NON_EC_PM_POLLUTANT_ID),
            process_id: i32::from(START_EXHAUST_PROCESS_ID),
            model_year_id: 2020,
            year_id: 2020,
            fuel_effect_ratio: 1.5,
        });
        let out = BasicStartPmEmissionCalculator::run(&inputs, &constants);
        assert_eq!(out.len(), 1);
        let expected = 1.5 * 10.0 * (0.04_f64 * 22.0).exp();
        assert!((out[0].emission_quant - expected).abs() < 1e-9);
    }

    #[test]
    fn ec_and_non_ec_pollutants_flow_independently() {
        // Add an Elemental-Carbon (11202) flow alongside the Non-EC one.
        let (mut inputs, constants) = single_flow_inputs();
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 1,
            source_type_id: 21,
            pol_process_id: EC_PM_START_POL_PROCESS_ID,
            op_mode_id: 101,
            op_mode_fraction: 0.5,
        });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            source_bin_id: 100,
            pol_process_id: EC_PM_START_POL_PROCESS_ID,
            op_mode_id: 101,
            age_group_id: 3,
            mean_base_rate: 1.0,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 500,
                pol_process_id: EC_PM_START_POL_PROCESS_ID,
                source_bin_id: 100,
                source_bin_activity_fraction: 1.0,
            });
        inputs
            .pollutant_process_model_year
            .push(PollutantProcessModelYearRow {
                pol_process_id: EC_PM_START_POL_PROCESS_ID,
                model_year_id: 2020,
                model_year_group_id: 7,
            });
        inputs
            .pollutant_process_mapped_model_year
            .push(PollutantProcessMappedModelYearRow {
                pol_process_id: EC_PM_START_POL_PROCESS_ID,
                model_year_id: 2020,
                model_year_group_id: 7,
            });
        inputs.start_temp_adjustment.push(StartTempAdjustmentRow {
            pol_process_id: EC_PM_START_POL_PROCESS_ID,
            fuel_type_id: 1,
            model_year_group_id: 7,
            op_mode_id: 101,
            temp_adjust_term_a: Some(0.04),
            temp_adjust_term_b: Some(1.0),
            temp_adjust_term_c: Some(0.0),
        });
        inputs
            .pollutant_process_assoc
            .push(PollutantProcessAssocRow {
                pol_process_id: EC_PM_START_POL_PROCESS_ID,
                pollutant_id: i32::from(EC_PM_POLLUTANT_ID),
                process_id: i32::from(START_EXHAUST_PROCESS_ID),
            });

        let out = BasicStartPmEmissionCalculator::run(&inputs, &constants);
        assert_eq!(out.len(), 2);
        let mut pollutants: Vec<i32> = out.iter().map(|r| r.pollutant_id).collect();
        pollutants.sort_unstable();
        assert_eq!(pollutants, vec![112, 118]);
    }
}
