//! Port of `RefuelingLossCalculator.java` and
//! `database/RefuelingLossCalculator.sql` — .
//!
//! `RefuelingLossCalculator` computes **Total Gaseous Hydrocarbon (THC)**
//! emissions for the two refueling processes — the fuel vapour displaced from a
//! vehicle's tank as liquid fuel pours in (**Refueling Displacement Vapor
//! Loss**, process 18) and the liquid fuel spilled at the nozzle (**Refueling
//! Spillage Loss**, process 19). It is one of the MOVES evaporative-emission
//! calculators.
//!
//! # Chained calculator
//!
//! `RefuelingLossCalculator` is a *chained* calculator.
//! `RefuelingLossCalculator.subscribeToMe` does **not** subscribe to the
//! MasterLoop; instead it chains itself onto the calculators that produce Total
//! Energy Consumption (pollutant 91) for the running (1), start (2),
//! extended-idle (90) and auxiliary-power (91) exhaust processes — in the
//! rates-first engine that is `BaseRateCalculator` — and runs inside the same
//! master-loop pass. `calculator-dag.json` records this as
//! `subscribes_directly: false`, `subscriptions: []`,
//! `depends_on: ["BaseRateCalculator"]`; the [`Calculator`] metadata methods
//! mirror it ([`subscriptions`](Calculator::subscriptions) is empty,
//! [`upstream`](Calculator::upstream) names `BaseRateCalculator`). This is the
//! `SO2Calculator` chained shape, not the `LiquidLeakingCalculator`
//! direct-subscriber shape.
//!
//! # What it computes
//!
//! Both refueling losses scale the Total Energy Consumption the vehicle burned
//! by a per-unit-fuel loss rate, converting energy to fuel volume through the
//! fuel's energy content and density:
//!
//! ```text
//! emissionQuant = lossRate × energy ÷ (energyContent × fuelDensity)
//! ```
//!
//! For **displacement vapour** the rate is a temperature-driven exponential,
//! adjusted for refueling control technology and Stage II programs:
//!
//! ```text
//! displacedVaporRate = exp(A + B×tankTemperatureDif + C×refuelingTemperature
//! + D×averageRVP) [floored]
//! adjustedVaporRate = displacedVaporRate × (1−P) × (1−T)
//! + controlledRefuelingRate × (1−P) × T
//! ```
//!
//! For **spillage** the rate is a flat base rate, adjusted the same way:
//!
//! ```text
//! adjustedSpillRate = (1−Pspill) × ((1−T) × refuelingSpillRate)
//! ```
//!
//! * `refuelingTemperature` — the ambient hourly temperature, optionally
//! re-expressed by the 2008 California study relation `20.30 + 0.81 × t`
//! with `t` clamped to `[vaporLowTLimit, vaporHighTLimit]`.
//! * `tankTemperatureDif` — `vaporTermE × refuelingTemperature + vaporTermF`,
//! clamped to `[0, tankTDiffLimit]`.
//! * `averageRVP` — the market-share-weighted Reid vapour pressure of the
//! `(month, fuelType)` fuel supply.
//! * `T` — `refuelingTechAdjustment`, the control-technology penetration.
//! * `P` / `Pspill` — `refuelingVaporProgramAdjust` / `refuelingSpillProgramAdjust`,
//! the Stage II vapour-recovery program reductions.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! [`RefuelingLossCalculator::calculate`] ports
//! `RefuelingLossCalculator.sql`'s "Processing" section. The SQL builds five
//! working tables; the port folds them into three index maps and one join
//! loop:
//!
//! | SQL working table / step | This port |
//! |--------------------------|-----------|
//! | `RefuelingAverageRVP` (REFEC-2) | `average_rvp` → `(monthID, fuelTypeID) → averageRVP` |
//! | `RefuelingTemp` (REFEC-1, 2) | `refueling_temp` → `fuelTypeID → [(monthID, hourID, displacedVaporRate)]` |
//! | `RefuelingDisplacement` (REFEC-3, 4) | `refueling_displacement` → `DisplacementKey → adjustedVaporRate` |
//! | `RefuelingSpillage` (REFEC-5, 6) | `refueling_spillage` → `SpillageKey → adjustedSpillRate` |
//! | `RefuelingWorkerOutputTemp` + `MOVESWorkerOutput` (REFEC-7, 8) | the [`calculate`](RefuelingLossCalculator::calculate) join loop → `Vec<`[`RefuelingEmissionRow`]`>` |
//!
//! `RefuelingTemp` is the cross product of the run's `(month, hour)`
//! temperatures and the per-fuel `RefuelingFactors`. `RefuelingAverageRVP` sums
//! `RVP × marketShare` over the fuel supply, then defaults every remaining
//! `(month, fuelType)` to `0`. `RefuelingDisplacement` and `RefuelingSpillage`
//! apply the technology and program adjustments. REFEC-7/8 joins each Total
//! Energy Consumption row to a rate and converts it to a THC quantity.
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key is
//! dropped; the port reproduces that with map lookups that skip on a miss.
//!
//! # `RefuelingCountyYear` fuel-type expansion
//!
//! `CountyYear` carries one `(refuelingVaporProgramAdjust,
//! refuelingSpillProgramAdjust)` pair per run county/year — the Stage II
//! reductions, which apply to gasoline. The displacement section uses that pair
//! directly. The spillage section first `ALTER`s a `fuelTypeID` column onto
//! `RefuelingCountyYear` (defaulting existing rows to gasoline, id 1),
//! duplicates the gasoline row as E85 (id 5, which most Stage II programs also
//! cover), and inserts zero-adjustment rows for fuel types 2, 3 and 9. The port
//! reproduces this with a `SPILLAGE_COUNTY_YEAR_FUEL_TYPES` table: the spillage
//! step synthesises the five `(fuelType, programAdjust)` rows from each input
//! `RefuelingCountyYear` row rather than mutating shared state.
//!
//! # Scope of this port
//!
//! [`calculate`](RefuelingLossCalculator::calculate) is the SQL "Processing"
//! section. Its [`RefuelingLossInputs`] argument is the set of tables the SQL's
//! "Create Remote Tables" / "Extract Data" sections produce, as plain row
//! vectors; a future (`DataFrameStore`) wiring populates it from the
//! per-run filtered execution database.
//!
//! Several things the SQL does are *not* the algorithm and are left to that
//! wiring:
//!
//! * The "Extract Data" `WHERE` clauses — `RefuelingControlTechnology` and
//! `SourceTypeTechAdjustment` filtered to processes 18/19 and a 40-year model
//! range, `RefuelingFuelType` aggregated from the run's fuel supply with
//! `energyContent > 0` and `fuelDensity > 0`, the location/time tables
//! filtered to the iteration county, zone and year — are the data-plane
//! contract. `calculate` treats its inputs as already so filtered.
//! * The Java `doExecute` enables the `RefuelingDisplacementVaporLoss` /
//! `RefuelingSpillageLoss` script sections per the RunSpec's requested
//! processes, populating `##refuelingDisplacement.pollutantIDs##` /
//! `##refuelingSpillage.pollutantIDs##` accordingly. `calculate` reproduces
//! that gate through [`RefuelingLossInputs::refueling_displacement_pollutant`]
//! and [`refueling_spillage_pollutant`](RefuelingLossInputs::refueling_spillage_pollutant):
//! the SQL emits one output row per `(workerOutputTemp row × pollutant)`, so
//! an empty pollutant set yields no rows for that process — exactly as a
//! disabled section does.
//! * `SCC` is written `NULL` and `MOVESRunID` / `iterationID` are left to the
//! insert's column defaults; none is modelled, matching `SO2Calculator`'s
//! treatment of its pass-through columns.
//!
//! # Fidelity notes
//!
//! `RefuelingLossCalculator.sql` stores `RefuelingTemp.refuelingTemperature`,
//! `.tankTemperatureDif` and `.displacedVaporRate`, `RefuelingAverageRVP.averageRVP`,
//! `RefuelingDisplacement.adjustedVaporRate` and `RefuelingSpillage.adjustedSpillRate`
//! as `FLOAT` (32-bit), while MariaDB evaluates the arithmetic in `DOUBLE`.
//! This port sums and multiplies in `f64` end to end, so it does not reproduce
//! the `f32` truncation MOVES applies when it writes those intermediates — a
//! sub-`1e-7` relative drift. Reproducing it bug-for-bug is the calculator
//! integration validation call (), matching the `SO2Calculator` /
//! `LiquidLeakingCalculator` precedent. The `RefuelingFactors` terms,
//! `marketShare`, `RVP`, `energyContent` and `fuelDensity` are likewise `FLOAT`
//! columns, but they are model *inputs* — already `f32`-quantised before
//! [`calculate`](RefuelingLossCalculator::calculate) sees them.
//! `RefuelingZoneMonthHour.temperature` and `MOVESWorkerOutput.emissionQuant` /
//! `.emissionRate` are `DOUBLE`.
//!
//! `RefuelingFuelFormulation.RVP` is nullable. A SQL `NULL` `RVP` contributes
//! nothing to `sum(RVP × marketShare)`, which is identical to contributing
//! `0.0`, so the port models `RVP` as a plain `f64` and the data-plane wiring
//! substitutes `0.0` for a `NULL`.
//!
//! There are no integer/integer divisions: the only divisor,
//! `energyContent × fuelDensity`, is `FLOAT × FLOAT`, so the MariaDB
//! `div_precision_increment` rounding gotcha does not arise. The
//! `ageID = yearID − modelYearID` relation is exact integer arithmetic. The
//! `RefuelingFuelType` extract keeps only `energyContent > 0` and
//! `fuelDensity > 0` rows, so the divisor is strictly positive; a zero divisor
//! is unreachable on extract-conformant input.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm is
//! fully ported and unit-tested on
//! [`calculate`](RefuelingLossCalculator::calculate); `execute` is a documented
//! shell returning an empty [`CalculatorOutput`]. Once the data plane exists,
//! `execute` materialises a [`RefuelingLossInputs`] from `ctx.tables()`, calls
//! [`calculate`](RefuelingLossCalculator::calculate), and writes the rows back
//! to `MOVESWorkerOutput`.

use std::collections::HashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `RefuelingLossCalculator`
/// entry in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "RefuelingLossCalculator";

/// Total Gaseous Hydrocarbons — `Pollutant` id 1, the pollutant both refueling
/// processes produce. `CalculatorInfo.txt` records two `Registration`
/// directives, both for this pollutant.
const THC_POLLUTANT: PollutantId = PollutantId(1);

/// Refueling Displacement Vapor Loss — `Process` id 18. The output rows for
/// this process carry `processID = 18`, a literal the SQL stamps into
/// `RefuelingWorkerOutputTemp`.
const DISPLACEMENT_PROCESS_ID: i32 = 18;

/// Refueling Spillage Loss — `Process` id 19. The output rows for this process
/// carry `processID = 19`, a literal the SQL stamps into
/// `RefuelingWorkerOutputTemp`.
const SPILLAGE_PROCESS_ID: i32 = 19;

/// Total Energy Consumption — `Pollutant` id 91. The energy rows the refueling
/// formulae consume are the `MOVESWorkerOutput` records for this pollutant.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

/// The exhaust processes whose Total Energy Consumption the refueling
/// calculation chains off — running (1), start (2), extended-idle (90) and
/// auxiliary-power (91). The SQL filters `MOVESWorkerOutput` with
/// `mwo.processID IN (1, 2, 90, 91)`; these are the Java `sourceProcessIDs`.
const ENERGY_SOURCE_PROCESS_IDS: [i32; 4] = [1, 2, 90, 91];

/// `(fuelTypeID, retains the Stage II program adjustment)` rows the
/// `RefuelingSpillageLoss` section synthesises onto `RefuelingCountyYear`.
///
/// The SQL `ALTER`s a `fuelTypeID` column in (existing rows default to
/// gasoline, id 1), duplicates the gasoline row as E85 (id 5 — most Stage II
/// programs cover E85 too, so it keeps the adjustment), and inserts
/// zero-adjustment rows for fuel types 2, 3 and 9. See the [module
/// documentation](self).
const SPILLAGE_COUNTY_YEAR_FUEL_TYPES: [(i32, bool); 5] =
    [(1, true), (5, true), (2, false), (3, false), (9, false)];

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `RefuelingLossCalculator.sql`'s
// "Create Remote Tables" / "Extract Data" sections produce that feed the
// "Processing" section. Following the convention, every `INT`/`SMALLINT`
// identifier is an `i32` and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only
// the columns the refueling algorithm reads are modelled.
// ===========================================================================

/// One `RefuelingZoneMonthHour` row — the iteration zone's ambient hourly
/// temperature.
///
/// The SQL extracts `ZoneMonthHour` for the iteration zone and the run's
/// months and hours; `zoneID` is constant across the extract and is not
/// modelled. The "Processing" section reads only `temperature`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingZoneMonthHourRow {
 /// `monthID`.
    pub month_id: i32,
 /// `hourID`.
    pub hour_id: i32,
 /// `temperature` — the ambient hourly temperature in °F. `DOUBLE` in MOVES.
    pub temperature: f64,
}

/// One `RefuelingFactors` row — the per-fuel-type refueling-emission
/// coefficients.
///
/// `RefuelingFactors` is keyed by `fuelTypeID`. The `…CV` uncertainty columns
/// and `defaultFormulationID` are not read by the "Processing" section and are
/// not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingFactorsRow {
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `vaporTermA` — the constant term of the displaced-vapour exponential.
    pub vapor_term_a: f64,
 /// `vaporTermB` — the `tankTemperatureDif` coefficient.
    pub vapor_term_b: f64,
 /// `vaporTermC` — the `refuelingTemperature` coefficient.
    pub vapor_term_c: f64,
 /// `vaporTermD` — the `averageRVP` coefficient.
    pub vapor_term_d: f64,
 /// `vaporTermE` — the `refuelingTemperature` coefficient of
 /// `tankTemperatureDif`.
    pub vapor_term_e: f64,
 /// `vaporTermF` — the constant term of `tankTemperatureDif`.
    pub vapor_term_f: f64,
 /// `vaporLowTLimit` — the lower bound of the refueling-temperature clamp.
 /// A value of `0` (with `vaporHighTLimit` also `0`) disables the clamp.
    pub vapor_low_t_limit: f64,
 /// `vaporHighTLimit` — the upper bound of the refueling-temperature clamp.
    pub vapor_high_t_limit: f64,
 /// `tankTDiffLimit` — the upper bound of `tankTemperatureDif`.
    pub tank_t_diff_limit: f64,
 /// `minimumRefuelingVaporLoss` — the floor applied to `displacedVaporRate`.
 /// A value `≤ −1` floors the rate to `0` instead.
    pub minimum_refueling_vapor_loss: f64,
 /// `refuelingSpillRate` — the unadjusted spillage base rate.
    pub refueling_spill_rate: f64,
}

/// One `RefuelingFuelSupply` row — a fuel formulation's market share of a
/// `(month group)` fuel supply.
///
/// The SQL extracts `FuelSupply` filtered to the run's fuel region and year, so
/// `fuelRegionID` and `fuelYearID` are constant across the extract and are not
/// modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingFuelSupplyRow {
 /// `monthGroupID` — joins to [`RefuelingMonthOfAnyYearRow::month_group_id`].
    pub month_group_id: i32,
 /// `fuelFormulationID` — joins to
 /// [`RefuelingFuelFormulationRow::fuel_formulation_id`].
    pub fuel_formulation_id: i32,
 /// `marketShare` — the formulation's share of the fuel supply.
    pub market_share: f64,
}

/// One `RefuelingFuelFormulation` row — a fuel formulation's Reid vapour
/// pressure.
///
/// `RefuelingFuelFormulation` carries the full fuel-property column set; the
/// "Processing" section reads only `RVP` (and `fuelSubtypeID` to join).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingFuelFormulationRow {
 /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
 /// `fuelSubtypeID` — joins to [`RefuelingFuelSubtypeRow::fuel_subtype_id`].
    pub fuel_subtype_id: i32,
 /// `RVP` — Reid vapour pressure. Nullable in MOVES; a `NULL` is supplied as
 /// `0.0` (see the [module documentation](self)).
    pub rvp: f64,
}

/// One `RefuelingMonthOfAnyYear` row — the `monthID → monthGroupID` mapping.
///
/// Each month group spans several months, so the `RefuelingAverageRVP`
/// aggregation fans a `monthGroup`-keyed fuel supply out across every month of
/// the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefuelingMonthOfAnyYearRow {
 /// `monthID`.
    pub month_id: i32,
 /// `monthGroupID`.
    pub month_group_id: i32,
}

/// One `RefuelingFuelSubtype` row — the `fuelSubtypeID → fuelTypeID` mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefuelingFuelSubtypeRow {
 /// `fuelSubtypeID`.
    pub fuel_subtype_id: i32,
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
}

/// One `RefuelingControlTechnology` row — the refueling-control penetration and
/// controlled rate for a `(process, modelYear, regClass, sourceType, fuelType,
/// age)` cell.
///
/// The SQL extracts the rows with `processID IN (18, 19)`; the displacement
/// section reads the `processID = 18` rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingControlTechnologyRow {
 /// `processID`.
    pub process_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `regClassID`.
    pub reg_class_id: i32,
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `ageID`.
    pub age_id: i32,
 /// `refuelingTechAdjustment` — the control-technology penetration `T`.
    pub refueling_tech_adjustment: f64,
 /// `controlledRefuelingRate` — the displaced-vapour rate of a fully
 /// controlled vehicle.
    pub controlled_refueling_rate: f64,
}

/// One `RefuelingCountyYear` row — the Stage II program reductions for the run
/// county and year.
///
/// The SQL extracts `CountyYear` filtered to the iteration county and year, so
/// the table carries one row (or none); see the [module documentation](self).
/// The "Processing" section reads only the two program-adjustment columns/// `countyID` and `yearID` are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingCountyYearRow {
 /// `refuelingVaporProgramAdjust` — the Stage II displacement-vapour
 /// reduction `P`.
    pub refueling_vapor_program_adjust: f64,
 /// `refuelingSpillProgramAdjust` — the Stage II spillage reduction `Pspill`.
    pub refueling_spill_program_adjust: f64,
}

/// One `SourceTypeTechAdjustment` row — the refueling-control penetration for a
/// `(process, sourceType, modelYear)` cell.
///
/// The SQL extracts the rows with `processID IN (18, 19)`; the spillage section
/// reads the `processID = 19` rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeTechAdjustmentRow {
 /// `processID`.
    pub process_id: i32,
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `refuelingTechAdjustment` — the control-technology penetration `T`.
    pub refueling_tech_adjustment: f64,
}

/// One `RefuelingFuelType` row — the market-weighted energy content and density
/// of a `(fuelType, month)` fuel supply.
///
/// `RefuelingFuelType` is a computed extract: `energyContent` is
/// `sum(marketShare × subtype energyContent)` over the run's fuel supply and
/// `fuelDensity` comes from `FuelType`. The extract keeps only rows with
/// `energyContent > 0` and `fuelDensity > 0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingFuelTypeRow {
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `monthID`.
    pub month_id: i32,
 /// `energyContent` — the market-weighted fuel energy content.
    pub energy_content: f64,
 /// `fuelDensity` — the fuel density.
    pub fuel_density: f64,
}

/// One `MOVESWorkerOutput` energy row — a Total Energy Consumption record the
/// upstream chained calculator (`BaseRateCalculator`) emitted.
///
/// The refueling calculation reads only the rows with `pollutantID = 91` and
/// `processID IN (1, 2, 90, 91)`; [`calculate`](RefuelingLossCalculator::calculate)
/// applies that filter, so an input may carry other rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EnergyRow {
 /// `pollutantID` — the refueling calculation uses only `91`.
    pub pollutant_id: i32,
 /// `processID` — the exhaust process the energy was consumed in.
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
 /// `emissionQuant` — the Total Energy Consumption quantity. `DOUBLE`.
    pub emission_quant: f64,
 /// `emissionRate` — the Total Energy Consumption rate. `DOUBLE`.
    pub emission_rate: f64,
}

/// Inputs to [`RefuelingLossCalculator::calculate`] — the tables the SQL's
/// "Create Remote Tables" / "Extract Data" sections produce, as plain row
/// vectors.
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct RefuelingLossInputs {
 /// `RefuelingZoneMonthHour` rows — the iteration zone's hourly
 /// temperatures.
    pub refueling_zone_month_hour: Vec<RefuelingZoneMonthHourRow>,
 /// `RefuelingFactors` rows — the per-fuel-type coefficients.
    pub refueling_factors: Vec<RefuelingFactorsRow>,
 /// `RefuelingFuelSupply` rows — the fuel supply for `averageRVP`.
    pub refueling_fuel_supply: Vec<RefuelingFuelSupplyRow>,
 /// `RefuelingFuelFormulation` rows — the fuel formulations' `RVP`.
    pub refueling_fuel_formulation: Vec<RefuelingFuelFormulationRow>,
 /// `RefuelingMonthOfAnyYear` rows — the `monthID → monthGroupID` mapping.
    pub refueling_month_of_any_year: Vec<RefuelingMonthOfAnyYearRow>,
 /// `RefuelingFuelSubtype` rows — the `fuelSubtypeID → fuelTypeID` mapping.
    pub refueling_fuel_subtype: Vec<RefuelingFuelSubtypeRow>,
 /// `RefuelingControlTechnology` rows — the displacement control technology.
    pub refueling_control_technology: Vec<RefuelingControlTechnologyRow>,
 /// `RefuelingCountyYear` rows — the run county/year Stage II reductions.
    pub refueling_county_year: Vec<RefuelingCountyYearRow>,
 /// `SourceTypeTechAdjustment` rows — the spillage control technology.
    pub source_type_tech_adjustment: Vec<SourceTypeTechAdjustmentRow>,
 /// `RefuelingFuelType` rows — the market-weighted fuel energy and density.
    pub refueling_fuel_type: Vec<RefuelingFuelTypeRow>,
 /// `RefuelingDisplacementPollutant` — the pollutant ids the displacement
 /// output is cross-joined with (the `##refuelingDisplacement.pollutantIDs##`
 /// set, normally `[1]`). An empty set yields no displacement rows.
    pub refueling_displacement_pollutant: Vec<i32>,
 /// `RefuelingSpillagePollutant` — the pollutant ids the spillage output is
 /// cross-joined with (the `##refuelingSpillage.pollutantIDs##` set, normally
 /// `[1]`). An empty set yields no spillage rows.
    pub refueling_spillage_pollutant: Vec<i32>,
 /// `MOVESWorkerOutput` rows. The calculation reads only the Total Energy
 /// Consumption rows (`pollutantID` 91, `processID` 1/2/90/91); any other
 /// row present is ignored, as the SQL's `WHERE` clause does.
    pub energy: Vec<EnergyRow>,
}

/// One refueling-loss emission record produced by the calculation — the
/// algorithm-bearing subset of the `MOVESWorkerOutput` row the SQL inserts.
///
/// `SCC` is written `NULL` by the SQL and `MOVESRunID` / `iterationID` are left
/// to the insert's column defaults; none is modelled (see the [module
/// documentation](self)). `pollutant_id` is the refueling pollutant (1, THC);
/// `process_id` is 18 (displacement) or 19 (spillage).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefuelingEmissionRow {
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
 /// `pollutantID` — the refueling pollutant (1, THC).
    pub pollutant_id: i32,
 /// `processID` — 18 (Refueling Displacement Vapor Loss) or 19 (Refueling
 /// Spillage Loss).
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
 /// `emissionQuant` — the refueling-loss emission quantity.
    pub emission_quant: f64,
 /// `emissionRate` — the refueling-loss emission rate.
    pub emission_rate: f64,
}

impl RefuelingEmissionRow {
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

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `RunSpecPollutantProcess` row — a `polProcessID` the RunSpec requests
/// output for. Used by `execute` to derive `refueling_displacement_pollutant`
/// and `refueling_spillage_pollutant`.
#[derive(Debug, Clone, Copy)]
struct RunSpecPollutantProcessRow {
    pol_process_id: i32,
}
impl TableRow for RunSpecPollutantProcessRow {
    fn table_name() -> &'static str {
        "RunSpecPollutantProcess"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("polProcessID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "polProcessID".into(),
                rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecPollutantProcess";
        let pp = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(RunSpecPollutantProcessRow {
                    pol_process_id: pp
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "polProcessID", "null value".into()))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingZoneMonthHourRow {
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
        let month = get_i32("monthID")?;
        let hour = get_i32("hourID")?;
        let temp = df
            .column("temperature")
            .map_err(|e| row_err(t, 0, "temperature", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "temperature", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingZoneMonthHourRow {
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    temperature: temp.get(i).ok_or_else(|| null("temperature"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingFactorsRow {
    fn table_name() -> &'static str {
        "RefuelingFactors"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("vaporTermA".into(), DataType::Float64),
            ("vaporTermB".into(), DataType::Float64),
            ("vaporTermC".into(), DataType::Float64),
            ("vaporTermD".into(), DataType::Float64),
            ("vaporTermE".into(), DataType::Float64),
            ("vaporTermF".into(), DataType::Float64),
            ("vaporLowTLimit".into(), DataType::Float64),
            ("vaporHighTLimit".into(), DataType::Float64),
            ("tankTDiffLimit".into(), DataType::Float64),
            ("minimumRefuelingVaporLoss".into(), DataType::Float64),
            ("refuelingSpillRate".into(), DataType::Float64),
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
                    "vaporTermA".into(),
                    rows.iter().map(|r| r.vapor_term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporTermB".into(),
                    rows.iter().map(|r| r.vapor_term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporTermC".into(),
                    rows.iter().map(|r| r.vapor_term_c).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporTermD".into(),
                    rows.iter().map(|r| r.vapor_term_d).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporTermE".into(),
                    rows.iter().map(|r| r.vapor_term_e).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporTermF".into(),
                    rows.iter().map(|r| r.vapor_term_f).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporLowTLimit".into(),
                    rows.iter()
                        .map(|r| r.vapor_low_t_limit)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "vaporHighTLimit".into(),
                    rows.iter()
                        .map(|r| r.vapor_high_t_limit)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankTDiffLimit".into(),
                    rows.iter()
                        .map(|r| r.tank_t_diff_limit)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "minimumRefuelingVaporLoss".into(),
                    rows.iter()
                        .map(|r| r.minimum_refueling_vapor_loss)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "refuelingSpillRate".into(),
                    rows.iter()
                        .map(|r| r.refueling_spill_rate)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RefuelingFactors";
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
        let fuel_type_id = get_i32("fuelTypeID")?;
        let a = get_f64("vaporTermA")?;
        let b = get_f64("vaporTermB")?;
        let c = get_f64("vaporTermC")?;
        let d = get_f64("vaporTermD")?;
        let e_col = get_f64("vaporTermE")?;
        let f_col = get_f64("vaporTermF")?;
        let low = get_f64("vaporLowTLimit")?;
        let high = get_f64("vaporHighTLimit")?;
        let tdiff = get_f64("tankTDiffLimit")?;
        let min_loss = get_f64("minimumRefuelingVaporLoss")?;
        let spill = get_f64("refuelingSpillRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingFactorsRow {
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    vapor_term_a: a.get(i).ok_or_else(|| null("vaporTermA"))?,
                    vapor_term_b: b.get(i).ok_or_else(|| null("vaporTermB"))?,
                    vapor_term_c: c.get(i).ok_or_else(|| null("vaporTermC"))?,
                    vapor_term_d: d.get(i).ok_or_else(|| null("vaporTermD"))?,
                    vapor_term_e: e_col.get(i).ok_or_else(|| null("vaporTermE"))?,
                    vapor_term_f: f_col.get(i).ok_or_else(|| null("vaporTermF"))?,
                    vapor_low_t_limit: low.get(i).ok_or_else(|| null("vaporLowTLimit"))?,
                    vapor_high_t_limit: high.get(i).ok_or_else(|| null("vaporHighTLimit"))?,
                    tank_t_diff_limit: tdiff.get(i).ok_or_else(|| null("tankTDiffLimit"))?,
                    minimum_refueling_vapor_loss: min_loss
                        .get(i)
                        .ok_or_else(|| null("minimumRefuelingVaporLoss"))?,
                    refueling_spill_rate: spill.get(i).ok_or_else(|| null("refuelingSpillRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingFuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
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
        let mg = get_i32("monthGroupID")?;
        let ff = get_i32("fuelFormulationID")?;
        let ms = df
            .column("marketShare")
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingFuelSupplyRow {
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                    fuel_formulation_id: ff.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    market_share: ms.get(i).ok_or_else(|| null("marketShare"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingFuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("RVP".into(), DataType::Float64),
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
                Series::new(
                    "RVP".into(),
                    rows.iter().map(|r| r.rvp).collect::<Vec<f64>>(),
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
        let ff = get_i32("fuelFormulationID")?;
        let fs = get_i32("fuelSubtypeID")?;
        let rvp = df
            .column("RVP")
            .map_err(|e| row_err(t, 0, "RVP", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "RVP", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingFuelFormulationRow {
                    fuel_formulation_id: ff.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_subtype_id: fs.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    rvp: rvp.get(i).ok_or_else(|| null("RVP"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingMonthOfAnyYearRow {
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
        let month = get_i32("monthID")?;
        let mg = get_i32("monthGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingMonthOfAnyYearRow {
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingFuelSubtypeRow {
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
        let fst = get_i32("fuelSubtypeID")?;
        let ft = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingFuelSubtypeRow {
                    fuel_subtype_id: fst.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingControlTechnologyRow {
    fn table_name() -> &'static str {
        "RefuelingControlTechnology"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("refuelingTechAdjustment".into(), DataType::Float64),
            ("controlledRefuelingRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
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
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "refuelingTechAdjustment".into(),
                    rows.iter()
                        .map(|r| r.refueling_tech_adjustment)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "controlledRefuelingRate".into(),
                    rows.iter()
                        .map(|r| r.controlled_refueling_rate)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RefuelingControlTechnology";
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
        let proc = get_i32("processID")?;
        let my = get_i32("modelYearID")?;
        let rc = get_i32("regClassID")?;
        let st = get_i32("sourceTypeID")?;
        let ft = get_i32("fuelTypeID")?;
        let age = get_i32("ageID")?;
        let adj = get_f64("refuelingTechAdjustment")?;
        let rate = get_f64("controlledRefuelingRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingControlTechnologyRow {
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    refueling_tech_adjustment: adj
                        .get(i)
                        .ok_or_else(|| null("refuelingTechAdjustment"))?,
                    controlled_refueling_rate: rate
                        .get(i)
                        .ok_or_else(|| null("controlledRefuelingRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingCountyYearRow {
    fn table_name() -> &'static str {
        "CountyYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("refuelingVaporProgramAdjust".into(), DataType::Float64),
            ("refuelingSpillProgramAdjust".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "refuelingVaporProgramAdjust".into(),
                    rows.iter()
                        .map(|r| r.refueling_vapor_program_adjust)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "refuelingSpillProgramAdjust".into(),
                    rows.iter()
                        .map(|r| r.refueling_spill_program_adjust)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "CountyYear";
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let vapor = get_f64("refuelingVaporProgramAdjust")?;
        let spill = get_f64("refuelingSpillProgramAdjust")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingCountyYearRow {
                    refueling_vapor_program_adjust: vapor
                        .get(i)
                        .ok_or_else(|| null("refuelingVaporProgramAdjust"))?,
                    refueling_spill_program_adjust: spill
                        .get(i)
                        .ok_or_else(|| null("refuelingSpillProgramAdjust"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeTechAdjustmentRow {
    fn table_name() -> &'static str {
        "SourceTypeTechAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("refuelingTechAdjustment".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "refuelingTechAdjustment".into(),
                    rows.iter()
                        .map(|r| r.refueling_tech_adjustment)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeTechAdjustment";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let proc = get_i32("processID")?;
        let st = get_i32("sourceTypeID")?;
        let my = get_i32("modelYearID")?;
        let adj = df
            .column("refuelingTechAdjustment")
            .map_err(|e| row_err(t, 0, "refuelingTechAdjustment", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "refuelingTechAdjustment", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeTechAdjustmentRow {
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    refueling_tech_adjustment: adj
                        .get(i)
                        .ok_or_else(|| null("refuelingTechAdjustment"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingFuelTypeRow {
    fn table_name() -> &'static str {
        "RefuelingFuelType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("energyContent".into(), DataType::Float64),
            ("fuelDensity".into(), DataType::Float64),
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
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "energyContent".into(),
                    rows.iter().map(|r| r.energy_content).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "fuelDensity".into(),
                    rows.iter().map(|r| r.fuel_density).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RefuelingFuelType";
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
        let month = get_i32("monthID")?;
        let ec = get_f64("energyContent")?;
        let fd = get_f64("fuelDensity")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingFuelTypeRow {
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    energy_content: ec.get(i).ok_or_else(|| null("energyContent"))?,
                    fuel_density: fd.get(i).ok_or_else(|| null("fuelDensity"))?,
                })
            })
            .collect()
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
        let pol = get_i32("pollutantID")?;
        let proc = get_i32("processID")?;
        let yr = get_i32("yearID")?;
        let mo = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let st = get_i32("stateID")?;
        let co = get_i32("countyID")?;
        let zo = get_i32("zoneID")?;
        let lk = get_i32("linkID")?;
        let sty = get_i32("sourceTypeID")?;
        let rc = get_i32("regClassID")?;
        let ft = get_i32("fuelTypeID")?;
        let my = get_i32("modelYearID")?;
        let rt = get_i32("roadTypeID")?;
        let eq = get_f64("emissionQuant")?;
        let er = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EnergyRow {
                    pollutant_id: pol.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: st.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: co.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zo.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: lk.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: sty.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: rt.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: er.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RefuelingEmissionRow {
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
        let yr = get_i32("yearID")?;
        let mo = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let st = get_i32("stateID")?;
        let co = get_i32("countyID")?;
        let zo = get_i32("zoneID")?;
        let lk = get_i32("linkID")?;
        let pol = get_i32("pollutantID")?;
        let proc = get_i32("processID")?;
        let sty = get_i32("sourceTypeID")?;
        let rc = get_i32("regClassID")?;
        let ft = get_i32("fuelTypeID")?;
        let my = get_i32("modelYearID")?;
        let rt = get_i32("roadTypeID")?;
        let eq = get_f64("emissionQuant")?;
        let er = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RefuelingEmissionRow {
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: st.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: co.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zo.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: lk.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: pol.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: sty.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: rt.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: er.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// Key into `RefuelingDisplacement` (REFEC-3, 4) — the seven columns the
/// REFEC-7 energy join matches a `MOVESWorkerOutput` row on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DisplacementKey {
    model_year_id: i32,
    reg_class_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    age_id: i32,
    month_id: i32,
    hour_id: i32,
}

/// Key into `RefuelingSpillage` (REFEC-5, 6) — the three columns the REFEC-7
/// energy join matches a `MOVESWorkerOutput` row on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SpillageKey {
    fuel_type_id: i32,
    source_type_id: i32,
    model_year_id: i32,
}

/// One `RefuelingTemp` cell, reduced to the column the displacement join needs:
/// the `(month, hour)` coordinate and the finished `displacedVaporRate`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RefuelingTempCell {
    month_id: i32,
    hour_id: i32,
 /// `displacedVaporRate` — post-floor (REFEC-2).
    displaced_vapor_rate: f64,
}

/// Build `RefuelingAverageRVP` — SQL "Processing" step REFEC-2's first two
/// inserts.
///
/// Sums `RVP × marketShare` over the fuel supply joined `RefuelingFuelSupply` →
/// `RefuelingFuelFormulation` → `RefuelingFuelSubtype`, fanned across every
/// month of the supply row's month group, keyed by `(monthID, fuelTypeID)`.
/// Then every remaining `(monthID, fuelTypeID)` over the cross product of
/// `RefuelingMonthOfAnyYear` and `RefuelingFactors` defaults to `0.0` — the
/// SQL's `INSERT IGNORE` fill.
fn average_rvp(inputs: &RefuelingLossInputs) -> HashMap<(i32, i32), f64> {
 // RefuelingFuelFormulation by fuelFormulationID (the table's key).
    let formulation_by_id: HashMap<i32, &RefuelingFuelFormulationRow> = inputs
        .refueling_fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
 // RefuelingFuelSubtype's fuelSubtypeID → fuelTypeID (the table's key).
    let fuel_type_by_subtype: HashMap<i32, i32> = inputs
        .refueling_fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst.fuel_type_id))
        .collect();
 // RefuelingMonthOfAnyYear grouped monthGroupID → [monthID]: a month group
 // spans several months, so a monthGroup-keyed fuel supply fans out.
    let mut months_by_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for moay in &inputs.refueling_month_of_any_year {
        months_by_group
            .entry(moay.month_group_id)
            .or_default()
            .push(moay.month_id);
    }

    let mut rvp: HashMap<(i32, i32), f64> = HashMap::new();
    for fs in &inputs.refueling_fuel_supply {
 // INNER JOIN RefuelingFuelFormulation ON fuelFormulationID.
        let Some(ff) = formulation_by_id.get(&fs.fuel_formulation_id) else {
            continue;
        };
 // INNER JOIN RefuelingFuelSubtype ON fuelSubtypeID.
        let Some(&fuel_type_id) = fuel_type_by_subtype.get(&ff.fuel_subtype_id) else {
            continue;
        };
 // INNER JOIN RefuelingMonthOfAnyYear ON monthGroupID.
        let Some(month_ids) = months_by_group.get(&fs.month_group_id) else {
            continue;
        };
        for &month_id in month_ids {
 *rvp.entry((month_id, fuel_type_id)).or_default() += ff.rvp * fs.market_share;
        }
    }

 // Default fill: every (monthID ∈ RefuelingMonthOfAnyYear, fuelTypeID ∈
 // RefuelingFactors) absent above gets averageRVP = 0.0.
    for moay in &inputs.refueling_month_of_any_year {
        for factors in &inputs.refueling_factors {
            rvp.entry((moay.month_id, factors.fuel_type_id))
                .or_insert(0.0);
        }
    }
    rvp
}

/// Build `RefuelingTemp` — SQL "Processing" steps REFEC-1 and REFEC-2.
///
/// Cross-joins the run's `(month, hour)` temperatures (`RefuelingZoneMonthHour`)
/// with the per-fuel `RefuelingFactors`, derives the refueling temperature, the
/// tank-temperature difference and the displaced-vapour rate, and groups the
/// finished rates by `fuelTypeID` for the displacement join. The
/// `RefuelingTemp` unique index is `(monthID, hourID, fuelTypeID)`; the cross
/// join of distinct `(month, hour)` and distinct `fuelTypeID` rows produces
/// exactly one cell per key.
fn refueling_temp(
    inputs: &RefuelingLossInputs,
    average_rvp: &HashMap<(i32, i32), f64>,
) -> HashMap<i32, Vec<RefuelingTempCell>> {
    let mut temp_by_fuel: HashMap<i32, Vec<RefuelingTempCell>> = HashMap::new();
 // CROSS JOIN RefuelingZoneMonthHour, RefuelingFactors.
    for rzmh in &inputs.refueling_zone_month_hour {
        for factors in &inputs.refueling_factors {
 // REFEC-1: refuelingTemperature. The 2008 California study relation
 // 20.30 + 0.81 × t applies only when both temperature limits are
 // set; t is then clamped to [vaporLowTLimit, vaporHighTLimit].
            let refueling_temperature =
                if factors.vapor_high_t_limit != 0.0 && factors.vapor_low_t_limit != 0.0 {
                    let clamped = factors
                        .vapor_low_t_limit
                        .max(rzmh.temperature)
                        .min(factors.vapor_high_t_limit);
                    20.30 + 0.81 * clamped
                } else {
                    rzmh.temperature
                };

 // REFEC-1: tankTemperatureDif, clamped to [0, tankTDiffLimit].
            let raw_tank_dif = factors.vapor_term_e * refueling_temperature + factors.vapor_term_f;
            let tank_temperature_dif = if raw_tank_dif >= factors.tank_t_diff_limit {
                factors.tank_t_diff_limit
            } else if raw_tank_dif <= 0.0 {
                0.0
            } else {
                raw_tank_dif
            };

 // REFEC-2: displacedVaporRate. The SQL UPDATE fires only where a
 // RefuelingAverageRVP row matches (monthID, fuelTypeID); a miss
 // leaves the column at its 0.0 default. The default fill in
 // `average_rvp` makes a miss reachable only for a month absent from
 // RefuelingMonthOfAnyYear.
            let displaced_raw = match average_rvp.get(&(rzmh.month_id, factors.fuel_type_id)) {
                Some(&avg) => (factors.vapor_term_a
                    + factors.vapor_term_b * tank_temperature_dif
                    + factors.vapor_term_c * refueling_temperature
                    + factors.vapor_term_d * avg)
                    .exp(),
                None => 0.0,
            };

 // REFEC-2: floor displacedVaporRate at minimumRefuelingVaporLoss
 // (or 0 when that sentinel is ≤ −1).
            let displaced_vapor_rate = if displaced_raw < factors.minimum_refueling_vapor_loss
                || factors.minimum_refueling_vapor_loss <= -1.0
            {
                if factors.minimum_refueling_vapor_loss <= -1.0 {
                    0.0
                } else {
                    factors.minimum_refueling_vapor_loss
                }
            } else {
                displaced_raw
            };

            temp_by_fuel
                .entry(factors.fuel_type_id)
                .or_default()
                .push(RefuelingTempCell {
                    month_id: rzmh.month_id,
                    hour_id: rzmh.hour_id,
                    displaced_vapor_rate,
                });
        }
    }
    temp_by_fuel
}

/// Build `RefuelingDisplacement` — SQL "Processing" steps REFEC-3 and REFEC-4.
///
/// Cross-joins each `RefuelingCountyYear` row with the `processID = 18`
/// `RefuelingControlTechnology` rows, joined to `RefuelingTemp` on
/// `fuelTypeID`, and applies the technology and Stage II program adjustments.
/// The `RefuelingDisplacement` unique index is `(modelYearID, regClassID,
/// sourceTypeID, fuelTypeID, ageID, monthID, hourID)`; that index and the
/// `RefuelingCountyYear` cross join together imply at most one
/// `RefuelingCountyYear` row, so the map insert never drops a distinct cell.
fn refueling_displacement(
    inputs: &RefuelingLossInputs,
    temp_by_fuel: &HashMap<i32, Vec<RefuelingTempCell>>,
) -> HashMap<DisplacementKey, f64> {
    let mut displacement: HashMap<DisplacementKey, f64> = HashMap::new();
 // CROSS JOIN RefuelingCountyYear — the run's single county/year row.
    for rcy in &inputs.refueling_county_year {
        for rct in &inputs.refueling_control_technology {
 // WHERE rct.processID = 18.
            if rct.process_id != DISPLACEMENT_PROCESS_ID {
                continue;
            }
 // INNER JOIN RefuelingTemp ON rct.fuelTypeID = rt.fuelTypeID.
            let Some(cells) = temp_by_fuel.get(&rct.fuel_type_id) else {
                continue;
            };
            for cell in cells {
 // adjustedVaporRate = displacedVaporRate × (1−P) × (1−T)
 // + controlledRefuelingRate × (1−P) × T.
                let adjusted_vapor_rate = cell.displaced_vapor_rate
 * (1.0 - rcy.refueling_vapor_program_adjust)
 * (1.0 - rct.refueling_tech_adjustment)
                    + rct.controlled_refueling_rate
 * (1.0 - rcy.refueling_vapor_program_adjust)
 * rct.refueling_tech_adjustment;
                displacement.insert(
                    DisplacementKey {
                        model_year_id: rct.model_year_id,
                        reg_class_id: rct.reg_class_id,
                        source_type_id: rct.source_type_id,
                        fuel_type_id: rct.fuel_type_id,
                        age_id: rct.age_id,
                        month_id: cell.month_id,
                        hour_id: cell.hour_id,
                    },
                    adjusted_vapor_rate,
                );
            }
        }
    }
    displacement
}

/// Build `RefuelingSpillage` — SQL "Processing" steps REFEC-5 and REFEC-6.
///
/// Synthesises the five fuel-type `RefuelingCountyYear` rows
/// ([`SPILLAGE_COUNTY_YEAR_FUEL_TYPES`]) from each input county/year row, joins
/// them to `RefuelingFactors` on `fuelTypeID`, cross-joins the `processID = 19`
/// `SourceTypeTechAdjustment` rows, and applies the technology and Stage II
/// program adjustments. The `RefuelingSpillage` unique index is `(fuelTypeID,
/// sourceTypeID, modelYearID)`; as with the displacement table, that index
/// implies at most one input county/year row.
fn refueling_spillage(inputs: &RefuelingLossInputs) -> HashMap<SpillageKey, f64> {
 // RefuelingFactors by fuelTypeID (the table's key).
    let factors_by_fuel: HashMap<i32, &RefuelingFactorsRow> = inputs
        .refueling_factors
        .iter()
        .map(|f| (f.fuel_type_id, f))
        .collect();

    let mut spillage: HashMap<SpillageKey, f64> = HashMap::new();
    for rcy in &inputs.refueling_county_year {
 // The ALTER + INSERT fan-out: gasoline (1) and E85 (5) keep the run's
 // refuelingSpillProgramAdjust; fuel types 2, 3 and 9 zero it.
        for &(fuel_type_id, retains_program_adjust) in &SPILLAGE_COUNTY_YEAR_FUEL_TYPES {
            let spill_program_adjust = if retains_program_adjust {
                rcy.refueling_spill_program_adjust
            } else {
                0.0
            };
 // INNER JOIN RefuelingFactors ON RefuelingCountyYear.fuelTypeID.
            let Some(factors) = factors_by_fuel.get(&fuel_type_id) else {
                continue;
            };
            for stta in &inputs.source_type_tech_adjustment {
 // WHERE SourceTypeTechAdjustment.processID = 19.
                if stta.process_id != SPILLAGE_PROCESS_ID {
                    continue;
                }
 // adjustedSpillRate = (1−Pspill) × ((1−T) × refuelingSpillRate).
                let adjusted_spill_rate = (1.0 - spill_program_adjust)
 * ((1.0 - stta.refueling_tech_adjustment) * factors.refueling_spill_rate);
                spillage.insert(
                    SpillageKey {
                        fuel_type_id,
                        source_type_id: stta.source_type_id,
                        model_year_id: stta.model_year_id,
                    },
                    adjusted_spill_rate,
                );
            }
        }
    }
    spillage
}

/// Stamp one [`RefuelingEmissionRow`] from a Total Energy Consumption row and a
/// computed `(pollutant, process, quantity, rate)` — the REFEC-8 insert into
/// `MOVESWorkerOutput`. The dimension columns are carried verbatim from the
/// energy row.
fn emission_row(
    energy: &EnergyRow,
    pollutant_id: i32,
    process_id: i32,
    emission_quant: f64,
    emission_rate: f64,
) -> RefuelingEmissionRow {
    RefuelingEmissionRow {
        year_id: energy.year_id,
        month_id: energy.month_id,
        day_id: energy.day_id,
        hour_id: energy.hour_id,
        state_id: energy.state_id,
        county_id: energy.county_id,
        zone_id: energy.zone_id,
        link_id: energy.link_id,
        pollutant_id,
        process_id,
        source_type_id: energy.source_type_id,
        reg_class_id: energy.reg_class_id,
        fuel_type_id: energy.fuel_type_id,
        model_year_id: energy.model_year_id,
        road_type_id: energy.road_type_id,
        emission_quant,
        emission_rate,
    }
}

/// The MOVES refueling-loss calculator.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`RefuelingLossInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct RefuelingLossCalculator;

impl RefuelingLossCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Compute the refueling-loss emission rows — the port of the
 /// `RefuelingLossCalculator.sql` "Processing" section.
 ///
 /// Each Total Energy Consumption row (`MOVESWorkerOutput`, pollutant 91,
 /// process 1/2/90/91) that resolves a `RefuelingFuelType` cell yields a
 /// displacement row when its dimension cell resolves a `RefuelingDisplacement`
 /// rate, and a spillage row when it resolves a `RefuelingSpillage` rate /// every SQL join is an `INNER JOIN`. Each such row is emitted once per
 /// pollutant in the matching `Refueling…Pollutant` set, so an empty set
 /// suppresses that process (the script's section gate). The result is
 /// sorted by its integer dimension columns for deterministic output; MOVES
 /// leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &RefuelingLossInputs) -> Vec<RefuelingEmissionRow> {
 // --- REFEC-1, 2: RefuelingAverageRVP and RefuelingTemp --------------
        let average_rvp = average_rvp(inputs);
        let temp_by_fuel = refueling_temp(inputs, &average_rvp);

 // --- REFEC-3..6: the two adjusted-rate working tables --------------
        let displacement = refueling_displacement(inputs, &temp_by_fuel);
        let spillage = refueling_spillage(inputs);

 // RefuelingFuelType by (fuelTypeID, monthID) — the REFEC-7 energy join.
        let fuel_type_by_key: HashMap<(i32, i32), &RefuelingFuelTypeRow> = inputs
            .refueling_fuel_type
            .iter()
            .map(|rft| ((rft.fuel_type_id, rft.month_id), rft))
            .collect();

 // --- REFEC-7, 8: join each energy row to a rate --------------------
        let mut out: Vec<RefuelingEmissionRow> = Vec::new();
        for mwo in &inputs.energy {
 // WHERE mwo.processID IN (1, 2, 90, 91) AND mwo.pollutantID = 91.
            if mwo.pollutant_id != TOTAL_ENERGY_POLLUTANT_ID
                || !ENERGY_SOURCE_PROCESS_IDS.contains(&mwo.process_id)
            {
                continue;
            }
 // INNER JOIN RefuelingFuelType ON (fuelTypeID, monthID).
            let Some(rft) = fuel_type_by_key.get(&(mwo.fuel_type_id, mwo.month_id)) else {
                continue;
            };
 // energyContent and fuelDensity are extract-filtered > 0, so the
 // divisor is strictly positive (see the module documentation).
            let divisor = rft.energy_content * rft.fuel_density;
            let age_id = mwo.year_id - mwo.model_year_id;

 // Refueling Displacement Vapor Loss (process 18).
            let displacement_key = DisplacementKey {
                model_year_id: mwo.model_year_id,
                reg_class_id: mwo.reg_class_id,
                source_type_id: mwo.source_type_id,
                fuel_type_id: mwo.fuel_type_id,
                age_id,
                month_id: mwo.month_id,
                hour_id: mwo.hour_id,
            };
            if let Some(&adjusted_vapor_rate) = displacement.get(&displacement_key) {
                let emission_quant = adjusted_vapor_rate * mwo.emission_quant / divisor;
                let emission_rate = adjusted_vapor_rate * mwo.emission_rate / divisor;
                for &pollutant_id in &inputs.refueling_displacement_pollutant {
                    out.push(emission_row(
                        mwo,
                        pollutant_id,
                        DISPLACEMENT_PROCESS_ID,
                        emission_quant,
                        emission_rate,
                    ));
                }
            }

 // Refueling Spillage Loss (process 19).
            let spillage_key = SpillageKey {
                fuel_type_id: mwo.fuel_type_id,
                source_type_id: mwo.source_type_id,
                model_year_id: mwo.model_year_id,
            };
            if let Some(&adjusted_spill_rate) = spillage.get(&spillage_key) {
                let emission_quant = adjusted_spill_rate * mwo.emission_quant / divisor;
                let emission_rate = adjusted_spill_rate * mwo.emission_rate / divisor;
                for &pollutant_id in &inputs.refueling_spillage_pollutant {
                    out.push(emission_row(
                        mwo,
                        pollutant_id,
                        SPILLAGE_PROCESS_ID,
                        emission_quant,
                        emission_rate,
                    ));
                }
            }
        }

 // Stable sort by the dimension columns: two output rows can share a
 // dimension key (energy rows for different source processes collapse
 // onto the same output process), and a stable sort keeps them in
 // energy-input order.
        out.sort_by_key(RefuelingEmissionRow::dimension_key);
        out
    }
}

/// `RefuelingLossCalculator` is a chained calculator — `subscribes_directly:
/// false` in `calculator-dag.json` — so it declares no MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The two `(pollutant, process)` pairs the calculator registers.
///
/// Total Gaseous Hydrocarbons (1) for Refueling Displacement Vapor Loss (18)
/// and Refueling Spillage Loss (19) — the two `Registration` directives
/// recorded for `RefuelingLossCalculator` in `CalculatorInfo.txt`
/// (`registrations_count: 2` in `calculator-dag.json`).
static REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: THC_POLLUTANT,
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: THC_POLLUTANT,
        process_id: ProcessId(19),
    },
];

/// The upstream calculator `RefuelingLossCalculator` chains off/// `BaseRateCalculator`, which produces the Total Energy Consumption
/// (pollutant 91) records the refueling formulae consume. `calculator-dag.json`
/// records it as `depends_on: ["BaseRateCalculator"]`.
static UPSTREAM: &[&str] = &["BaseRateCalculator"];

/// Default-DB tables the refueling computation consumes — the data tables the
/// SQL's "Extract Data" section pulls that feed the "Processing" section.
///
/// `RefuelingFuelType` is a computed extract (`sum(marketShare × energyContent)`
/// over the fuel supply joined to `FuelType` for `fuelDensity`), so its
/// constituent default-DB tables — `FuelType`, `FuelSubtype`, `FuelFormulation`,
/// `FuelSupply`, `Year` — are listed. `RefuelingCountyYear` is extracted from
/// `CountyYear`. The SQL also joins the `RunSpec*` filter tables; those only
/// narrow the extract and do not feed the algorithm, so they are not listed
/// (matching `SO2Calculator`'s treatment of its `RunSpec*` joins).
static INPUT_TABLES: &[&str] = &[
    "CountyYear",
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "FuelType",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "Pollutant",
    "RefuelingControlTechnology",
    "RefuelingFactors",
    "SourceTypeTechAdjustment",
    "Year",
    "ZoneMonthHour",
];

impl Calculator for RefuelingLossCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

 /// `RefuelingLossCalculator` is a chained calculator: it does not subscribe
 /// to the MasterLoop directly but fires when its upstream
 /// `BaseRateCalculator` does. `calculator-dag.json` records
 /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

 /// `RefuelingLossCalculator` chains off `BaseRateCalculator` /// `calculator-dag.json` records `depends_on: ["BaseRateCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let rs_pol_processes =
            tables.iter_typed::<RunSpecPollutantProcessRow>("RunSpecPollutantProcess")?;
        let refueling_displacement_pollutant: Vec<i32> = rs_pol_processes
            .iter()
            .filter(|r| r.pol_process_id % 100 == DISPLACEMENT_PROCESS_ID)
            .map(|r| r.pol_process_id / 100)
            .collect();
        let refueling_spillage_pollutant: Vec<i32> = rs_pol_processes
            .iter()
            .filter(|r| r.pol_process_id % 100 == SPILLAGE_PROCESS_ID)
            .map(|r| r.pol_process_id / 100)
            .collect();
        let inputs = RefuelingLossInputs {
            refueling_zone_month_hour: tables
                .iter_typed::<RefuelingZoneMonthHourRow>("ZoneMonthHour")?,
            refueling_factors: tables.iter_typed::<RefuelingFactorsRow>("RefuelingFactors")?,
            refueling_fuel_supply: tables.iter_typed::<RefuelingFuelSupplyRow>("FuelSupply")?,
            refueling_fuel_formulation: tables
                .iter_typed::<RefuelingFuelFormulationRow>("FuelFormulation")?,
            refueling_month_of_any_year: tables
                .iter_typed::<RefuelingMonthOfAnyYearRow>("MonthOfAnyYear")?,
            refueling_fuel_subtype: tables.iter_typed::<RefuelingFuelSubtypeRow>("FuelSubtype")?,
            refueling_control_technology: tables
                .iter_typed::<RefuelingControlTechnologyRow>("RefuelingControlTechnology")?,
            refueling_county_year: tables.iter_typed::<RefuelingCountyYearRow>("CountyYear")?,
            source_type_tech_adjustment: tables
                .iter_typed::<SourceTypeTechAdjustmentRow>("SourceTypeTechAdjustment")?,
            refueling_fuel_type: tables.iter_typed::<RefuelingFuelTypeRow>("RefuelingFuelType")?,
            refueling_displacement_pollutant,
            refueling_spillage_pollutant,
            energy: tables.iter_typed::<EnergyRow>("MOVESWorkerOutput")?,
        };
        crate::wiring::emit_rows(self.calculate(&inputs))
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(RefuelingLossCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

 /// Assert `actual` matches `expected` within `f64` slack — the
 /// FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

 /// A `RefuelingFactors` row whose vapour terms are all zero — so
 /// `displacedVaporRate = exp(0) = 1.0` exactly — with a `refuelingSpillRate`
 /// of `2.0` and no temperature clamp or rate floor active. Tests tweak the
 /// fields they exercise.
    fn neutral_factors(fuel_type_id: i32) -> RefuelingFactorsRow {
        RefuelingFactorsRow {
            fuel_type_id,
            vapor_term_a: 0.0,
            vapor_term_b: 0.0,
            vapor_term_c: 0.0,
            vapor_term_d: 0.0,
            vapor_term_e: 0.0,
            vapor_term_f: 0.0,
            vapor_low_t_limit: 0.0,
            vapor_high_t_limit: 0.0,
            tank_t_diff_limit: 1.0e9,
            minimum_refueling_vapor_loss: 0.0,
            refueling_spill_rate: 2.0,
        }
    }

 /// A single Total Energy Consumption row for fuel type 1, month 7, hour 8,
 /// model year 2015 of run year 2020 (age 5) with `emissionQuant = 100.0`
 /// and `emissionRate = 4.0`.
    fn energy_row() -> EnergyRow {
        EnergyRow {
            pollutant_id: 91,
            process_id: 1,
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 1,
            model_year_id: 2015,
            road_type_id: 4,
            emission_quant: 100.0,
            emission_rate: 4.0,
        }
    }

 /// A minimal one-fuel / one-energy-row input. With neutral factors and zero
 /// adjustments the rates resolve exactly:
 ///
 /// * `displacedVaporRate = exp(0) = 1.0`, `adjustedVaporRate = 1.0`.
 /// * `adjustedSpillRate = (1−0) × ((1−0) × 2.0) = 2.0`.
 /// * `energyContent × fuelDensity = 2.0 × 5.0 = 10.0`.
 /// * displacement `emissionQuant = 1.0 × 100.0 / 10.0 = 10.0`,
 /// `emissionRate = 1.0 × 4.0 / 10.0 = 0.4`.
 /// * spillage `emissionQuant = 2.0 × 100.0 / 10.0 = 20.0`,
 /// `emissionRate = 2.0 × 4.0 / 10.0 = 0.8`.
    fn minimal_inputs() -> RefuelingLossInputs {
        RefuelingLossInputs {
            refueling_zone_month_hour: vec![RefuelingZoneMonthHourRow {
                month_id: 7,
                hour_id: 8,
                temperature: 75.0,
            }],
            refueling_factors: vec![neutral_factors(1)],
            refueling_fuel_supply: vec![],
            refueling_fuel_formulation: vec![],
 // Month 7 must appear so the averageRVP default fill reaches it,
 // letting the displacedVaporRate UPDATE fire.
            refueling_month_of_any_year: vec![RefuelingMonthOfAnyYearRow {
                month_id: 7,
                month_group_id: 3,
            }],
            refueling_fuel_subtype: vec![],
            refueling_control_technology: vec![RefuelingControlTechnologyRow {
                process_id: 18,
                model_year_id: 2015,
                reg_class_id: 30,
                source_type_id: 21,
                fuel_type_id: 1,
                age_id: 5,
                refueling_tech_adjustment: 0.0,
                controlled_refueling_rate: 0.0,
            }],
            refueling_county_year: vec![RefuelingCountyYearRow {
                refueling_vapor_program_adjust: 0.0,
                refueling_spill_program_adjust: 0.0,
            }],
            source_type_tech_adjustment: vec![SourceTypeTechAdjustmentRow {
                process_id: 19,
                source_type_id: 21,
                model_year_id: 2015,
                refueling_tech_adjustment: 0.0,
            }],
            refueling_fuel_type: vec![RefuelingFuelTypeRow {
                fuel_type_id: 1,
                month_id: 7,
                energy_content: 2.0,
                fuel_density: 5.0,
            }],
            refueling_displacement_pollutant: vec![1],
            refueling_spillage_pollutant: vec![1],
            energy: vec![energy_row()],
        }
    }

    #[test]
    fn calculate_minimal_input_yields_displacement_and_spillage_rows() {
        let rows = RefuelingLossCalculator.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 2);

 // Sorted by dimension key — process 18 sorts before 19.
        let displacement = rows[0];
        let spillage = rows[1];
        assert_eq!(displacement.process_id, 18);
        assert_eq!(spillage.process_id, 19);

 // Dimension columns are carried verbatim from the energy row.
        assert_eq!(displacement.year_id, 2020);
        assert_eq!(displacement.month_id, 7);
        assert_eq!(displacement.day_id, 5);
        assert_eq!(displacement.hour_id, 8);
        assert_eq!(displacement.state_id, 26);
        assert_eq!(displacement.county_id, 26_161);
        assert_eq!(displacement.zone_id, 261_610);
        assert_eq!(displacement.link_id, 5001);
        assert_eq!(displacement.source_type_id, 21);
        assert_eq!(displacement.reg_class_id, 30);
        assert_eq!(displacement.fuel_type_id, 1);
        assert_eq!(displacement.model_year_id, 2015);
        assert_eq!(displacement.road_type_id, 4);

 // Both processes relabel the pollutant to THC (1).
        assert_eq!(displacement.pollutant_id, 1);
        assert_eq!(spillage.pollutant_id, 1);

        assert_close(displacement.emission_quant, 10.0);
        assert_close(displacement.emission_rate, 0.4);
        assert_close(spillage.emission_quant, 20.0);
        assert_close(spillage.emission_rate, 0.8);
    }

    #[test]
    fn technology_and_program_adjustments_blend_the_displacement_rate() {
 // refuelingTechAdjustment = 0.25, controlledRefuelingRate = 4.0,
 // refuelingVaporProgramAdjust = 0.10:
 // adjustedVaporRate = 1.0 × 0.90 × 0.75 + 4.0 × 0.90 × 0.25
 // = 0.675 + 0.90 = 1.575
 // emissionQuant = 1.575 × 100.0 / 10.0 = 15.75
        let mut inputs = minimal_inputs();
        inputs.refueling_control_technology[0].refueling_tech_adjustment = 0.25;
        inputs.refueling_control_technology[0].controlled_refueling_rate = 4.0;
        inputs.refueling_county_year[0].refueling_vapor_program_adjust = 0.10;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
        assert_close(displacement.emission_quant, 15.75);
        assert_close(displacement.emission_rate, 1.575 * 4.0 / 10.0);
    }

    #[test]
    fn temperature_clamp_uses_the_california_study_relation() {
 // vaporLowTLimit = 60, vaporHighTLimit = 90, temperature = 120 → the
 // clamp pins the temperature to 90, then refuelingTemperature =
 // 20.30 + 0.81 × 90 = 93.2. With vaporTermC = 0.01 (other terms 0)
 // displacedVaporRate = exp(0.01 × 93.2). Energy 10 / divisor 10 makes
 // emissionQuant == displacedVaporRate.
        let mut inputs = minimal_inputs();
        inputs.refueling_factors[0].vapor_low_t_limit = 60.0;
        inputs.refueling_factors[0].vapor_high_t_limit = 90.0;
        inputs.refueling_factors[0].vapor_term_c = 0.01;
        inputs.refueling_zone_month_hour[0].temperature = 120.0;
        inputs.refueling_fuel_type[0].energy_content = 2.0;
        inputs.refueling_fuel_type[0].fuel_density = 5.0;
        inputs.energy[0].emission_quant = 10.0;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
        let clamped_refueling_temperature: f64 = 20.30 + 0.81 * 90.0;
        assert_close(
            displacement.emission_quant,
            (0.01 * clamped_refueling_temperature).exp(),
        );
    }

    #[test]
    fn tank_temperature_difference_is_clamped_to_its_limit() {
 // vaporTermE = 1.0, vaporTermF = 0.0 → raw tankTemperatureDif equals the
 // refueling temperature (75.0); tankTDiffLimit = 30 caps it at 30.
 // vaporTermB = 0.01 → displacedVaporRate = exp(0.01 × 30) = exp(0.3).
        let mut inputs = minimal_inputs();
        inputs.refueling_factors[0].vapor_term_e = 1.0;
        inputs.refueling_factors[0].vapor_term_b = 0.01;
        inputs.refueling_factors[0].tank_t_diff_limit = 30.0;
        inputs.refueling_fuel_type[0].energy_content = 2.0;
        inputs.refueling_fuel_type[0].fuel_density = 5.0;
        inputs.energy[0].emission_quant = 10.0;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
        assert_close(displacement.emission_quant, (0.01_f64 * 30.0).exp());
    }

    #[test]
    fn minimum_refueling_vapor_loss_floors_the_rate() {
 // displacedVaporRate would be exp(0) = 1.0, but minimumRefuelingVaporLoss
 // = 3.0 floors it to 3.0. emissionQuant = 3.0 × 100.0 / 10.0 = 30.0.
        let mut inputs = minimal_inputs();
        inputs.refueling_factors[0].minimum_refueling_vapor_loss = 3.0;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
        assert_close(displacement.emission_quant, 30.0);
    }

    #[test]
    fn negative_minimum_refueling_vapor_loss_zeroes_the_rate() {
 // minimumRefuelingVaporLoss ≤ −1 is the sentinel that zeroes
 // displacedVaporRate; the displacement emission is then 0.
        let mut inputs = minimal_inputs();
        inputs.refueling_factors[0].minimum_refueling_vapor_loss = -1.0;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
        assert_close(displacement.emission_quant, 0.0);
        assert_close(displacement.emission_rate, 0.0);
    }

    #[test]
    fn average_rvp_weights_rvp_by_market_share() {
 // Two formulations of fuel type 1, market shares 0.25 / 0.75, RVP 8 / 12:
 // averageRVP = 0.25 × 8 + 0.75 × 12 = 11.0
 // vaporTermD = 0.1 → displacedVaporRate = exp(0.1 × 11.0) = exp(1.1).
        let mut inputs = minimal_inputs();
        inputs.refueling_factors[0].vapor_term_d = 0.1;
        inputs.refueling_fuel_subtype = vec![RefuelingFuelSubtypeRow {
            fuel_subtype_id: 10,
            fuel_type_id: 1,
        }];
        inputs.refueling_fuel_formulation = vec![
            RefuelingFuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                rvp: 8.0,
            },
            RefuelingFuelFormulationRow {
                fuel_formulation_id: 101,
                fuel_subtype_id: 10,
                rvp: 12.0,
            },
        ];
        inputs.refueling_fuel_supply = vec![
            RefuelingFuelSupplyRow {
                month_group_id: 3,
                fuel_formulation_id: 100,
                market_share: 0.25,
            },
            RefuelingFuelSupplyRow {
                month_group_id: 3,
                fuel_formulation_id: 101,
                market_share: 0.75,
            },
        ];

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let displacement = rows.iter().find(|r| r.process_id == 18).unwrap();
 // emissionQuant = displacedVaporRate × 100.0 / 10.0 = exp(1.1) × 10.0.
        assert_close(displacement.emission_quant, 1.1_f64.exp() * 10.0);
    }

    #[test]
    fn spillage_zeroes_the_program_adjustment_for_non_gasoline_fuels() {
 // Fuel type 2 (diesel): the synthesised RefuelingCountyYear row carries
 // refuelingSpillProgramAdjust = 0 even though the run county/year sets
 // it to 0.5. adjustedSpillRate = (1−0) × ((1−0) × 2.0) = 2.0.
        let mut inputs = minimal_inputs();
        inputs.refueling_county_year[0].refueling_spill_program_adjust = 0.5;
        inputs.refueling_factors = vec![neutral_factors(2)];
        inputs.refueling_control_technology[0].fuel_type_id = 2;
        inputs.refueling_fuel_type[0].fuel_type_id = 2;
        inputs.source_type_tech_adjustment[0].source_type_id = 21;
        inputs.energy[0].fuel_type_id = 2;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let spillage = rows.iter().find(|r| r.process_id == 19).unwrap();
 // (1 − 0) × ((1 − 0) × 2.0) × 100.0 / 10.0 = 20.0.
        assert_close(spillage.emission_quant, 20.0);
    }

    #[test]
    fn spillage_applies_the_program_adjustment_to_gasoline() {
 // Fuel type 1 (gasoline) keeps the run's refuelingSpillProgramAdjust:
 // adjustedSpillRate = (1−0.5) × ((1−0) × 2.0) = 1.0
 // emissionQuant = 1.0 × 100.0 / 10.0 = 10.0
        let mut inputs = minimal_inputs();
        inputs.refueling_county_year[0].refueling_spill_program_adjust = 0.5;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let spillage = rows.iter().find(|r| r.process_id == 19).unwrap();
        assert_close(spillage.emission_quant, 10.0);
    }

    #[test]
    fn displacement_row_dropped_when_control_technology_is_missing() {
 // No RefuelingControlTechnology row → RefuelingDisplacement is empty →
 // the INNER JOIN drops the displacement output. Spillage still resolves.
        let mut inputs = minimal_inputs();
        inputs.refueling_control_technology.clear();

        let rows = RefuelingLossCalculator.calculate(&inputs);
        assert!(rows.iter().all(|r| r.process_id == 19));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn refueling_fuel_type_miss_drops_the_energy_row_entirely() {
 // RefuelingFuelType for a different month → the (fuelType, month) join
 // misses, so neither process produces a row.
        let mut inputs = minimal_inputs();
        inputs.refueling_fuel_type[0].month_id = 1;

        assert!(RefuelingLossCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn empty_displacement_pollutant_set_suppresses_displacement_rows() {
 // An empty ##refuelingDisplacement.pollutantIDs## set is how a disabled
 // script section reads — no displacement rows, spillage unaffected.
        let mut inputs = minimal_inputs();
        inputs.refueling_displacement_pollutant.clear();

        let rows = RefuelingLossCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].process_id, 19);
    }

    #[test]
    fn empty_spillage_pollutant_set_suppresses_spillage_rows() {
        let mut inputs = minimal_inputs();
        inputs.refueling_spillage_pollutant.clear();

        let rows = RefuelingLossCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].process_id, 18);
    }

    #[test]
    fn energy_rows_for_unrelated_pollutants_or_processes_are_ignored() {
        let mut inputs = minimal_inputs();
 // A non-energy pollutant and an out-of-set process are both dropped.
        inputs.energy.push(EnergyRow {
            pollutant_id: 2,
            ..energy_row()
        });
        inputs.energy.push(EnergyRow {
            process_id: 12,
            ..energy_row()
        });

 // Still exactly the two rows the one valid energy row produces.
        assert_eq!(RefuelingLossCalculator.calculate(&inputs).len(), 2);
    }

    #[test]
    fn extended_idle_energy_row_is_a_valid_source_process() {
 // Process 90 (extended idle) is in the source set; it still produces
 // refueling output, stamped with the refueling process id.
        let mut inputs = minimal_inputs();
        inputs.energy[0].process_id = 90;

        let rows = RefuelingLossCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let mut procs: Vec<i32> = rows.iter().map(|r| r.process_id).collect();
        procs.sort_unstable();
        assert_eq!(procs, vec![18, 19]);
    }

    #[test]
    fn no_county_year_row_yields_no_output() {
 // With no RefuelingCountyYear row both rate tables are empty, so the
 // calculator emits nothing — the empty-extract edge case.
        let mut inputs = minimal_inputs();
        inputs.refueling_county_year.clear();

        assert!(RefuelingLossCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn output_is_sorted_by_dimension_key() {
 // Two energy rows differing only in modelYearID; the output must be
 // ordered by the dimension key regardless of input order.
        let mut inputs = minimal_inputs();
        inputs
            .refueling_control_technology
            .push(RefuelingControlTechnologyRow {
                model_year_id: 2010,
                age_id: 10,
                ..inputs.refueling_control_technology[0]
            });
        inputs
            .source_type_tech_adjustment
            .push(SourceTypeTechAdjustmentRow {
                model_year_id: 2010,
                ..inputs.source_type_tech_adjustment[0]
            });
        inputs.energy = vec![
            EnergyRow {
                model_year_id: 2015,
                ..energy_row()
            },
            EnergyRow {
                model_year_id: 2010,
                ..energy_row()
            },
        ];

        let rows = RefuelingLossCalculator.calculate(&inputs);
        let keys: Vec<[i32; 15]> = rows
            .iter()
            .map(RefuelingEmissionRow::dimension_key)
            .collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(RefuelingLossCalculator.name(), "RefuelingLossCalculator");
        assert_eq!(RefuelingLossCalculator::NAME, "RefuelingLossCalculator");
    }

    #[test]
    fn calculator_is_a_chained_calculator_with_no_subscriptions() {
 // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(RefuelingLossCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_match_the_two_calculator_info_directives() {
 // CalculatorInfo.txt records two Registration directives: THC (1) for
 // Refueling Displacement Vapor Loss (18) and Refueling Spillage Loss
 // (19) — registrations_count 2 in calculator-dag.json.
        let regs = RefuelingLossCalculator.registrations();
        assert_eq!(regs.len(), 2);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(1)));
        let mut procs: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        procs.sort_unstable();
        assert_eq!(procs, vec![18, 19]);
    }

    #[test]
    fn calculator_chains_off_base_rate_calculator() {
 // calculator-dag.json records depends_on ["BaseRateCalculator"].
        assert_eq!(RefuelingLossCalculator.upstream(), &["BaseRateCalculator"]);
    }

    #[test]
    fn calculator_declares_input_tables() {
        let tables = RefuelingLossCalculator.input_tables();
        for expected in [
            "CountyYear",
            "FuelFormulation",
            "FuelSubtype",
            "FuelSupply",
            "FuelType",
            "MOVESWorkerOutput",
            "MonthOfAnyYear",
            "Pollutant",
            "RefuelingControlTechnology",
            "RefuelingFactors",
            "SourceTypeTechAdjustment",
            "Year",
            "ZoneMonthHour",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "ZoneMonthHour",
            RefuelingZoneMonthHourRow::into_dataframe(inputs.refueling_zone_month_hour.clone())
                .unwrap(),
        );
        store.insert(
            "RefuelingFactors",
            RefuelingFactorsRow::into_dataframe(inputs.refueling_factors.clone()).unwrap(),
        );
        store.insert(
            "FuelSupply",
            RefuelingFuelSupplyRow::into_dataframe(inputs.refueling_fuel_supply.clone()).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            RefuelingFuelFormulationRow::into_dataframe(inputs.refueling_fuel_formulation.clone())
                .unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            RefuelingMonthOfAnyYearRow::into_dataframe(inputs.refueling_month_of_any_year.clone())
                .unwrap(),
        );
        store.insert(
            "FuelSubtype",
            RefuelingFuelSubtypeRow::into_dataframe(inputs.refueling_fuel_subtype.clone()).unwrap(),
        );
        store.insert(
            "RefuelingControlTechnology",
            RefuelingControlTechnologyRow::into_dataframe(
                inputs.refueling_control_technology.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "CountyYear",
            RefuelingCountyYearRow::into_dataframe(inputs.refueling_county_year.clone()).unwrap(),
        );
        store.insert(
            "SourceTypeTechAdjustment",
            SourceTypeTechAdjustmentRow::into_dataframe(inputs.source_type_tech_adjustment.clone())
                .unwrap(),
        );
        store.insert(
            "RefuelingFuelType",
            RefuelingFuelTypeRow::into_dataframe(inputs.refueling_fuel_type.clone()).unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            EnergyRow::into_dataframe(inputs.energy.clone()).unwrap(),
        );
 // THC (pollutant 1) for process 18 → polProcessID = 118, for process 19 → polProcessID = 119
        store.insert(
            "RunSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![
                RunSpecPollutantProcessRow {
                    pol_process_id: 118,
                },
                RunSpecPollutantProcessRow {
                    pol_process_id: 119,
                },
            ])
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = RefuelingLossCalculator.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert!(
            df.height() > 0,
            "execute must return at least one output row"
        );
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "RefuelingLossCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
 // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(RefuelingLossCalculator);
        assert_eq!(calc.name(), "RefuelingLossCalculator");
    }
}
