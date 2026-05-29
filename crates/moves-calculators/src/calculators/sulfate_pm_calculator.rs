//! Port of `SulfatePMCalculator.java` and `database/SulfatePMCalculator.sql` —
//! migration plan Phase 3, Task 57.
//!
//! `SulfatePMCalculator` is the MOVES **PM2.5 speciation** calculator. Upstream
//! the rates engine produces two coarse exhaust-particulate tallies — unadjusted
//! elemental carbon (`Elemental Carbon`, pollutant 112) and the composite
//! non-EC particulate (`Composite - NonECPM`, pollutant 118). This calculator
//! splits the composite into its chemical species, the headline split being the
//! **sulfate fraction**, which depends on the sulfur content of the fuel
//! burned: a higher-sulfur fuel oxidises to more particulate sulfate.
//!
//! # Chained calculator
//!
//! `SulfatePMCalculator.subscribeToMe` does **not** subscribe to the MasterLoop;
//! it is a *chained* calculator that finds the calculators producing its EC
//! (112) and NonECPM (118) inputs and chains itself onto them, running in the
//! same master-loop pass. In the rates-first engine that producer is
//! `BaseRateCalculator`: `CalculatorInfo.txt` records `Chain SulfatePMCalculator
//! BaseRateCalculator` — `SulfatePMCalculator` depends on `BaseRateCalculator` —
//! and no `Subscribe` directive for `SulfatePMCalculator`. The [`Calculator`]
//! metadata mirrors that: [`subscriptions`](Calculator::subscriptions) is empty
//! and [`upstream`](Calculator::upstream) names `BaseRateCalculator`.
//!
//! # What it computes
//!
//! For each `(fuelType, sourceType, month, modelYear)` cell the calculator
//! first derives a market-share-weighted, fuel-sulfur-adjusted sulfate fraction
//! and a matching water (H2O aerosol) fraction:
//!
//! ```text
//! adjustment        = 1 + BaseFuelSulfateFraction × (sulfurLevel ÷ BaseFuelSulfurLevel − 1)
//! SulfateFraction   = Σ marketShare × SulfatenonECPMFraction × adjustment
//! H2OFraction       = Σ marketShare × H2ONonECPMFraction     × adjustment
//! ```
//!
//! It then splits the composite NonECPM (118) of every emission row into three
//! sub-species — particulate sulfate (115), water aerosol (119) and the
//! non-EC / non-sulfate residue (120, an internal pollutant) — applies a
//! general fuel-effect ratio to the residue, splits each species into its
//! exhaust and crankcase processes, sums the species back into an adjusted
//! NonECPM and an optional PM2.5 total, speciates everything into the trace
//! metals and organic carbon, and finally derives total organic matter (123)
//! and the non-EC / non-sulfate / non-organic residue (124).
//!
//! # Algorithm — the SQL script
//!
//! [`SulfatePMCalculator::calculate`] ports `SulfatePMCalculator.sql`. The
//! script's "Extract Data" section pre-aggregates the fuel-sulfur sulfate
//! fraction and the "Processing" section does the splitting; this port folds
//! both into one pass:
//!
//! | SQL step | This port |
//! |----------|-----------|
//! | `oneCountyYearSulfateFractions` (Extract Data) | `compute_sulfate_fractions` |
//! | `spmSplit1` + apply splits (118 → 115/119/120) | `calculate`, the NonECPM split loop |
//! | copy unadjusted EC (112) into `spmOutput` | `calculate`, the EC copy |
//! | apply general fuel-effect ratio | `calculate`, the fuel-ratio multiply |
//! | crankcase split → `spmOutput2` | `calculate`, the crankcase-split loop |
//! | `Section MakePM2.5Total` (110) | `sum_to_pollutant` |
//! | copy 112/115/119/120 to output | `calculate` |
//! | re-sum adjusted NonECPM (118) | `sum_to_pollutant` |
//! | speciate the trace species | `speciate` |
//! | Total Organic Matter (123) | `sum_to_pollutant` |
//! | NonECNonSO4NonOM PM (124) | `compute_non_ec_non_so4_non_om` |
//! | drop 120, conditionally drop 123/124 | `calculate`, the final retain |
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key is
//! dropped; the port reproduces that with map lookups that skip on a miss. The
//! general fuel-effect ratio is applied by a multi-table `UPDATE`, not a join —
//! a row with no matching ratio keeps its value unchanged.
//!
//! [`calculate`](SulfatePMCalculator::calculate) returns the **final state of
//! `MOVESWorkerOutput`**: the input rows minus the consumed EC and NonECPM,
//! plus every species the script inserts. Rows for pollutants this calculator
//! does not touch pass through unchanged.
//!
//! # Registrations
//!
//! `SulfatePMCalculator` carries 133 `Registration` directives in
//! `CalculatorInfo.txt` — 19 PM species (the 11 trace metals/ions 35, 36,
//! 51–59, plus 110, 111, 112, 115, 118, 119, 121, 122) for each of the 7
//! exhaust processes it handles (running 1, start 2, the three crankcase
//! processes 15/16/17, extended-idle 90 and auxiliary-power 91).
//! [`registrations`](Calculator::registrations) returns that 19 × 7 cross
//! product. The crankcase particulate pollutants 112/115/118 register here and
//! not to the superseded `CrankcaseEmissionCalculatorPM` — see that module.
//!
//! # Fidelity notes
//!
//! `SulfatePMCalculator.sql` writes every intermediate table
//! (`oneCountyYearSulfateFractions`, `sPMOneCountyYearGeneralFuelRatio`,
//! `crankcaseSplit`, `spmSplit1`) with `double` columns, and MariaDB evaluates
//! the arithmetic in `DOUBLE`; this port computes in `f64` end to end, so —
//! unlike the `SO2Calculator` port — there is no `FLOAT` intermediate column to
//! truncate. `crankcaseRatio`, `pmSpeciationFraction`, `marketShare` and
//! `sulfurLevel` are `FLOAT` model *inputs*, already `f32`-quantised before
//! [`calculate`](SulfatePMCalculator::calculate) sees them;
//! `MOVESWorkerOutput.emissionQuant` / `.emissionRate` are `DOUBLE`.
//!
//! `sulfurLevel ÷ BaseFuelSulfurLevel` divides two `DOUBLE` values, so the
//! MariaDB integer-division `div_precision_increment` rounding gotcha does not
//! arise. `BaseFuelSulfurLevel` is a physically positive base value and is
//! never zero in real data; a zero would yield a non-finite adjustment where
//! MariaDB's `x / 0` yields `NULL` — reachable only on degenerate input. The
//! SQL's `coalesce(ff.sulfurLevel, 0)` is reproduced by
//! [`FuelFormulationRow::sulfur_level`] being an [`Option`] defaulted to `0.0`,
//! and `greatest(…, 0)` by [`f64::max`].
//!
//! The SQL's `MakePM2.5Total` (110), `NonECPM` re-sum (118), `TOM` (123) and
//! `NonECNonSO4NonOM` (124) steps `GROUP BY` the full output dimension, which in
//! the source additionally keys on `SCC`, `engTechID`, `sectorID` and `hpID`.
//! Those four columns are `NULL` / `0` for the onroad master loop this
//! calculator runs in; following the `SO2Calculator` / `RefuelingLossCalculator`
//! treatment of pass-through columns they are not modelled, and the port groups
//! by the 14 modelled non-pollutant dimensions. `MOVESRunID` / `iterationID` /
//! `SCC` are likewise pass-through and unmodelled.
//!
//! The `NonECNonSO4NonOM` (124) step joins each residue row to a `PMSpeciation`
//! sub-aggregate keyed by `(processID, inputPollutantID, sourceTypeID,
//! fuelTypeID, modelYear range)` but does not constrain `inputPollutantID` in
//! the join, then multiplies a `GROUP BY` `SUM` by the non-aggregated
//! `ratio124`. In real data the organic-matter speciation has a single input
//! pollutant (120) and non-overlapping model-year ranges, so exactly one
//! sub-aggregate matches each residue row; `compute_non_ec_non_so4_non_om`
//! sums one `quantity × ratio124` contribution per match, which agrees with the
//! SQL whenever that holds.
//!
//! The result is sorted by its integer dimension columns for deterministic
//! output; MOVES leaves `MOVESWorkerOutput` physically unordered (no
//! `ORDER BY`).
//!
//! # Scope of this port — data plane (Task 50)
//!
//! [`calculate`](SulfatePMCalculator::calculate) is the numeric algorithm. Its
//! [`SulfatePmInputs`] argument is the set of tables the SQL reads, as plain
//! row vectors; a future Task 50 (`DataFrameStore`) wiring populates it from the
//! per-run filtered execution database. The SQL's "Extract Data" section also
//! narrows the source tables to the run's county, year and fuel region and
//! pre-aggregates the general fuel-effect ratio into
//! `sPMOneCountyYearGeneralFuelRatio`; that narrowing and the auxiliary
//! ratio aggregation are data-plane plumbing — [`SulfatePmInputs`] receives the
//! already-narrowed tables and the pre-aggregated ratio rows, matching the
//! `SO2Calculator` treatment of its `so2PMOneCountyYearGeneralFuelRatio`.
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands, so `execute` cannot yet read the input tables nor
//! emit `MOVESWorkerOutput`. The numeric algorithm is fully ported and
//! unit-tested on [`calculate`](SulfatePMCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises a [`SulfatePmInputs`] from
//! `ctx.tables()`, calls [`calculate`](SulfatePMCalculator::calculate), and
//! writes the rows back to `MOVESWorkerOutput`.

use rustc_hash::FxHashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `SulfatePMCalculator`
/// entry in the calculator-chain DAG.
const CALCULATOR_NAME: &str = "SulfatePMCalculator";

/// Elemental Carbon — `Pollutant` id 112. The unadjusted EC tally the upstream
/// rates calculator produces; this calculator copies it through the crankcase
/// split and speciation untouched in quantity.
const EC_POLLUTANT: i32 = 112;

/// Composite - NonECPM — `Pollutant` id 118. The coarse non-EC particulate the
/// calculator splits into sulfate (115), water (119) and the residue (120).
const NON_EC_PM_POLLUTANT: i32 = 118;

/// Sulfate Particulate — `Pollutant` id 115. The fuel-sulfur-dependent sulfate
/// fraction of the NonECPM split.
const SULFATE_POLLUTANT: i32 = 115;

/// H2O (aerosol) — `Pollutant` id 119. The particulate-water fraction of the
/// NonECPM split.
const H2O_POLLUTANT: i32 = 119;

/// NonECNonSO4PM — `Pollutant` id 120. An *internal* residue pollutant: the
/// non-EC, non-sulfate, non-water remainder of NonECPM. It is the only
/// pollutant the general fuel-effect ratio touches and is dropped from the
/// final output (the SQL's unconditional `delete … where pollutantID = 120`).
const NON_EC_NON_SO4_PM_POLLUTANT: i32 = 120;

/// Primary Exhaust PM2.5 - Total — `Pollutant` id 110. The sum of EC, sulfate,
/// water and the residue, produced only when the optional `MakePM2.5Total`
/// section is enabled.
const PM25_TOTAL_POLLUTANT: i32 = 110;

/// Organic Carbon — `Pollutant` id 111. One of the two organic species summed
/// into Total Organic Matter.
const ORGANIC_CARBON_POLLUTANT: i32 = 111;

/// Non-carbon Organic Matter (NCOM) — `Pollutant` id 122. The second organic
/// species summed into Total Organic Matter.
const NCOM_POLLUTANT: i32 = 122;

/// Total Organic Matter (TOM) — `Pollutant` id 123. The sum of organic carbon
/// (111) and NCOM (122).
const TOM_POLLUTANT: i32 = 123;

/// NonECNonSO4NonOM PM — `Pollutant` id 124. The residue (120) with its organic
/// matter removed: `120 × (1 − organic-matter speciation fraction)`.
const NON_EC_NON_SO4_NON_OM_POLLUTANT: i32 = 124;

/// Pollutants the `MakePM2.5Total` section sums into pollutant 110.
const PM25_TOTAL_INPUTS: [i32; 4] = [
    EC_POLLUTANT,
    NON_EC_NON_SO4_PM_POLLUTANT,
    SULFATE_POLLUTANT,
    H2O_POLLUTANT,
];

/// Pollutants the SQL copies verbatim from `spmOutput2` into `MOVESWorkerOutput`.
const COPIED_SPECIES: [i32; 4] = [
    EC_POLLUTANT,
    SULFATE_POLLUTANT,
    H2O_POLLUTANT,
    NON_EC_NON_SO4_PM_POLLUTANT,
];

/// Pollutants the SQL re-sums into the adjusted NonECPM (118).
const NON_EC_PM_INPUTS: [i32; 3] = [
    NON_EC_NON_SO4_PM_POLLUTANT,
    SULFATE_POLLUTANT,
    H2O_POLLUTANT,
];

/// Pollutants the SQL sums into Total Organic Matter (123).
const TOM_INPUTS: [i32; 2] = [ORGANIC_CARBON_POLLUTANT, NCOM_POLLUTANT];

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `SulfatePMCalculator.sql`
// reads. Following the Phase 3 convention every `INT`/`SMALLINT` identifier is
// an `i32` and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only the columns the
// algorithm reads are modelled.
// ===========================================================================

/// One `FuelSupply` row — a fuel formulation's market share in the run's fuel
/// region for a `(fuelYear, monthGroup)`.
///
/// The SQL extracts `FuelSupply` filtered to the run's single fuel region and
/// fuel year, so `fuelRegionID` and `fuelYearID` are run-constant and not
/// modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
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
    /// `fuelSubtypeID` — joins to [`FuelSubtypeRow::fuel_subtype_id`].
    pub fuel_subtype_id: i32,
    /// `sulfurLevel` — fuel sulfur content. The SQL reads it as
    /// `coalesce(sulfurLevel, 0)`, so a `NULL` is modelled as `None` and
    /// treated as `0.0`. `FLOAT` in MOVES.
    pub sulfur_level: Option<f64>,
}

/// One `FuelSubtype` row — a fuel subtype's parent fuel type.
///
/// The algorithm reads only the subtype → fuel-type mapping; the table's other
/// columns (energy content, etc.) are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubtypeRow {
    /// `fuelSubtypeID` — the subtype primary key.
    pub fuel_subtype_id: i32,
    /// `fuelTypeID` — the parent fuel type.
    pub fuel_type_id: i32,
}

/// One `sulfateFractions` row — the base sulfate and water fractions of NonECPM
/// for a `(process, fuelType, sourceType, model-year range)`, and the base-fuel
/// reference values the fuel-sulfur adjustment is relative to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SulfateFractionsRow {
    /// `processID` — the emission process the fractions apply to.
    pub process_id: i32,
    /// `fuelTypeID` — the fuel type the fractions apply to.
    pub fuel_type_id: i32,
    /// `sourceTypeID` — the source (vehicle) type the fractions apply to.
    pub source_type_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year window.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year window.
    pub max_model_year_id: i32,
    /// `SulfatenonECPMFraction` — base sulfate fraction of NonECPM.
    pub sulfate_non_ec_pm_fraction: f64,
    /// `H2ONonECPMFraction` — base water (aerosol) fraction of NonECPM.
    pub h2o_non_ec_pm_fraction: f64,
    /// `BaseFuelSulfateFraction` — the sensitivity of the sulfate fraction to
    /// the fuel sulfur level, relative to the base fuel.
    pub base_fuel_sulfate_fraction: f64,
    /// `BaseFuelSulfurLevel` — the sulfur level of the base fuel the fractions
    /// were measured against; the denominator of the adjustment.
    pub base_fuel_sulfur_level: f64,
}

/// One `MonthOfAnyYear` row — the `monthID → monthGroupID` mapping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthGroupRow {
    /// `monthID` — the calendar month.
    pub month_id: i32,
    /// `monthGroupID` — the month group it belongs to.
    pub month_group_id: i32,
}

/// One `sPMOneCountyYearGeneralFuelRatio` row — the market-share-weighted
/// general fuel-effect multiplier the SQL's "Extract Data" section
/// pre-aggregates.
///
/// Matches by `(fuelTypeID, sourceTypeID, pollutantID, processID)` plus
/// model-year and age ranges. Only the residue pollutant 120 carries
/// fuel-effect rows, so only the residue is affected.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeneralFuelRatioRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `pollutantID` — always 120 (NonECNonSO4PM) in this extract.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `minModelYearID` — inclusive lower bound of the applicable model years.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the applicable model years.
    pub max_model_year_id: i32,
    /// `minAgeID` — inclusive lower bound of the applicable vehicle ages.
    pub min_age_id: i32,
    /// `maxAgeID` — inclusive upper bound of the applicable vehicle ages.
    pub max_age_id: i32,
    /// `fuelEffectRatio` — the multiplier applied to the residue emission.
    pub fuel_effect_ratio: f64,
}

/// One `crankcaseSplit` row — the crankcase-to-exhaust split fraction for a
/// `(pollutant, process, sourceType, regClass, fuelType, model-year range)`.
///
/// The SQL builds this table from `crankcaseEmissionRatio`; `process_id` is
/// either the primary exhaust process or its crankcase process, and a single
/// emission row is split into one output row per matching `crankcaseSplit` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CrankcaseSplitRow {
    /// `processID` — the output process (a primary exhaust or crankcase
    /// process); the split row stamps this onto its output emission row.
    pub process_id: i32,
    /// `pollutantID` — the pollutant the split applies to.
    pub pollutant_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year window.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year window.
    pub max_model_year_id: i32,
    /// `crankcaseRatio` — the multiplier. `FLOAT` in MOVES; a model input.
    pub crankcase_ratio: f64,
}

/// One `PMSpeciation` row — the speciation fraction of an input particulate
/// pollutant into one output species.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PmSpeciationRow {
    /// `processID` — the emission process.
    pub process_id: i32,
    /// `inputPollutantID` — the particulate pollutant being speciated.
    pub input_pollutant_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year window.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year window.
    pub max_model_year_id: i32,
    /// `outputPollutantID` — the species produced.
    pub output_pollutant_id: i32,
    /// `pmSpeciationFraction` — the fraction of the input pollutant the species
    /// makes up. `FLOAT` in MOVES; a model input.
    pub pm_speciation_fraction: f64,
}

/// One `MOVESWorkerOutput` row — the subset of columns the algorithm reads and
/// writes.
///
/// The same shape serves the calculator's **input** (the EC and NonECPM
/// emission records the upstream calculator produced, plus any other pollutant
/// already in the table) and its **output** (the speciated rows the SQL writes
/// back). `MOVESRunID`, `iterationID`, `SCC`, `engTechID`, `sectorID` and
/// `hpID` are pass-through / un-modelled columns (see the [module
/// documentation](self)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRow {
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
    /// `emissionQuant` — the emission quantity. `DOUBLE` in MOVES.
    pub emission_quant: f64,
    /// `emissionRate` — the emission rate. `DOUBLE` in MOVES.
    pub emission_rate: f64,
}

impl EmissionRow {
    /// The full integer dimension tuple — every column except the two emission
    /// values. Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered.
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

    /// The integer dimension tuple **excluding** `pollutantID` — the `GROUP BY`
    /// key of the SQL's `MakePM2.5Total` / NonECPM-resum / TOM / NonECNonSO4NonOM
    /// steps, which sum several pollutants into one and stamp a literal
    /// `pollutantID` onto the result.
    fn non_pollutant_key(&self) -> [i32; 14] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.process_id,
            self.source_type_id,
            self.reg_class_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

/// Inputs to [`SulfatePMCalculator::calculate`] — the tables the SQL reads, as
/// plain row vectors, plus the run-context scalars its `##…##` placeholders
/// resolve.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct SulfatePmInputs {
    /// `FuelSupply` rows (single fuel region and fuel year).
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubtype` rows — the formulation → fuel-type mapping.
    pub fuel_subtype: Vec<FuelSubtypeRow>,
    /// `sulfateFractions` rows — the base sulfate/water fractions and the
    /// base-fuel reference values.
    pub sulfate_fractions: Vec<SulfateFractionsRow>,
    /// `MonthOfAnyYear` rows — the `monthID → monthGroupID` mapping.
    pub month_of_any_year: Vec<MonthGroupRow>,
    /// The model years the run covers — distinct `RunSpecModelYearAge.modelYearID`
    /// for the run year. The `sulfateFractions` model-year ranges are expanded
    /// onto these.
    pub run_spec_model_years: Vec<i32>,
    /// `sPMOneCountyYearGeneralFuelRatio` rows — the pre-aggregated general
    /// fuel-effect multiplier. May be empty: a cell with no matching ratio keeps
    /// its emission unchanged.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
    /// `crankcaseSplit` rows — the crankcase-to-exhaust split fractions.
    pub crankcase_split: Vec<CrankcaseSplitRow>,
    /// `PMSpeciation` rows — the species speciation fractions.
    pub pm_speciation: Vec<PmSpeciationRow>,
    /// `MOVESWorkerOutput` rows — the upstream emission records. The calculation
    /// consumes the EC (112) and NonECPM (118) rows and passes every other
    /// pollutant through unchanged.
    pub worker_output: Vec<EmissionRow>,
    /// The iteration's primary exhaust process — `context.iterProcess`.
    pub primary_process_id: i32,
    /// The iteration's crankcase process, if the primary process has one
    /// (running → 15, start → 16, extended-idle → 17; auxiliary-power has
    /// none).
    pub crankcase_process_id: Option<i32>,
    /// The processes for which the RunSpec requests Primary Exhaust PM2.5 -
    /// Total (110) — `##primaryAndCrankcaseProcessIDsForPM25Total##`. Empty
    /// disables the `MakePM2.5Total` section.
    pub pm25_total_process_ids: Vec<i32>,
    /// The `pollutantID × 100 + processID` values the RunSpec requests output
    /// for — `##polProcessIDs##`. A TOM (123) or NonECNonSO4NonOM (124) row is
    /// kept only if its composite id is in this set.
    pub output_pol_processes: Vec<i32>,
}

/// One `oneCountyYearSulfateFractions` cell — the market-share-weighted,
/// fuel-sulfur-adjusted sulfate and water fractions of one `(process, fuelType,
/// sourceType, month, modelYear)` group, and the unadjusted base fractions the
/// residue split uses.
#[derive(Debug, Clone, Copy, Default)]
struct SulfateFractionCell {
    /// `SulfateNonECPMFraction` — `Σ marketShare × SulfatenonECPMFraction ×
    /// adjustment`.
    sulfate_fraction: f64,
    /// `H2ONonECPMFraction` — `Σ marketShare × H2ONonECPMFraction ×
    /// adjustment`.
    h2o_fraction: f64,
    /// `UnadjustedSulfatenonECPMFraction` — the base sulfate fraction, unweighted
    /// and unadjusted, used by the residue (120) split.
    unadjusted_sulfate_fraction: f64,
    /// `UnadjustedH2ONonECPMFraction` — the base water fraction, unweighted and
    /// unadjusted, used by the residue (120) split.
    unadjusted_h2o_fraction: f64,
}

/// Key of a [`SulfateFractionCell`] — `(processID, fuelTypeID, sourceTypeID,
/// monthID, modelYearID)`.
type SulfateFractionKey = (i32, i32, i32, i32, i32);

/// The MOVES PM2.5 speciation calculator.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`SulfatePmInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct SulfatePMCalculator;

impl SulfatePMCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the speciated `MOVESWorkerOutput` — the port of
    /// `SulfatePMCalculator.sql`.
    ///
    /// Returns the final state of `MOVESWorkerOutput`: the input rows minus the
    /// consumed EC (112) and the NonECPM (118) of the iteration's processes,
    /// plus the EC, sulfate, water, adjusted NonECPM, optional PM2.5 total,
    /// trace species, Total Organic Matter and NonECNonSO4NonOM the script
    /// inserts. The internal residue pollutant 120 is always dropped; Total
    /// Organic Matter (123) and NonECNonSO4NonOM (124) are kept only when the
    /// RunSpec requests them. The result is sorted by its integer dimension
    /// columns for deterministic output.
    #[must_use]
    pub fn calculate(&self, inputs: &SulfatePmInputs) -> Vec<EmissionRow> {
        let fractions = compute_sulfate_fractions(inputs);

        // --- spmOutput: copy EC (112) and split NonECPM (118) --------------
        // The SQL copies every unadjusted EC row into spmOutput, then builds
        // spmSplit1 (three conversion fractions per oneCountyYearSulfateFractions
        // cell) and joins it to the NonECPM rows. This port folds spmSplit1 into
        // the lookup: each 118 row resolves its fraction cell and emits one
        // sulfate (115), one water (119) and one residue (120) row.
        let mut spm_output: Vec<EmissionRow> = Vec::new();
        for row in &inputs.worker_output {
            if row.pollutant_id == EC_POLLUTANT {
                spm_output.push(*row);
            }
        }
        for row in &inputs.worker_output {
            if row.pollutant_id != NON_EC_PM_POLLUTANT {
                continue;
            }
            // INNER JOIN spmSplit1 — a NonECPM row with no fraction cell for
            // its (process, fuel, source, month, modelYear) is dropped.
            let key: SulfateFractionKey = (
                row.process_id,
                row.fuel_type_id,
                row.source_type_id,
                row.month_id,
                row.model_year_id,
            );
            let Some(cell) = fractions.get(&key) else {
                continue;
            };
            // 115 = NonECPM × adjusted sulfate fraction,
            // 119 = NonECPM × adjusted water fraction,
            // 120 = NonECPM × greatest(1 − unadjusted water − unadjusted sulfate, 0).
            let residue_fraction =
                (1.0 - cell.unadjusted_h2o_fraction - cell.unadjusted_sulfate_fraction).max(0.0);
            for (out_pollutant, fraction) in [
                (SULFATE_POLLUTANT, cell.sulfate_fraction),
                (H2O_POLLUTANT, cell.h2o_fraction),
                (NON_EC_NON_SO4_PM_POLLUTANT, residue_fraction),
            ] {
                spm_output.push(EmissionRow {
                    pollutant_id: out_pollutant,
                    emission_quant: row.emission_quant * fraction,
                    emission_rate: row.emission_rate * fraction,
                    ..*row
                });
            }
        }

        // --- apply the general fuel-effect ratio ---------------------------
        // A multi-table UPDATE: only the residue (120) carries ratio rows, so
        // only the residue is rescaled; a row with no matching ratio is left
        // unchanged. Match by exact ids and model-year/age ranges.
        for row in &mut spm_output {
            let age = row.year_id - row.model_year_id;
            if let Some(gfr) = inputs.general_fuel_ratio.iter().find(|gfr| {
                gfr.fuel_type_id == row.fuel_type_id
                    && gfr.source_type_id == row.source_type_id
                    && gfr.pollutant_id == row.pollutant_id
                    && gfr.process_id == row.process_id
                    && gfr.min_model_year_id <= row.model_year_id
                    && gfr.max_model_year_id >= row.model_year_id
                    && gfr.min_age_id <= age
                    && gfr.max_age_id >= age
            }) {
                row.emission_quant *= gfr.fuel_effect_ratio;
                row.emission_rate *= gfr.fuel_effect_ratio;
            }
        }

        // --- crankcase split → spmOutput2 ----------------------------------
        // Each spmOutput row joins crankcaseSplit on (pollutant, fuel, source,
        // regClass) with the model year inside the split's window — the join
        // does not constrain process, so a primary-process row that matches a
        // primary and a crankcase split row yields one of each.
        let crankcase_index = index_crankcase_split(&inputs.crankcase_split);
        let mut spm_output2: Vec<EmissionRow> = Vec::new();
        for row in &spm_output {
            let Some(splits) = crankcase_index.get(&(
                row.pollutant_id,
                row.fuel_type_id,
                row.source_type_id,
                row.reg_class_id,
            )) else {
                continue;
            };
            for split in splits {
                if row.model_year_id < split.min_model_year_id
                    || row.model_year_id > split.max_model_year_id
                {
                    continue;
                }
                spm_output2.push(EmissionRow {
                    process_id: split.process_id,
                    emission_quant: row.emission_quant * split.crankcase_ratio,
                    emission_rate: row.emission_rate * split.crankcase_ratio,
                    ..*row
                });
            }
        }

        // --- assemble the final MOVESWorkerOutput --------------------------
        let mut primary_and_crankcase = vec![inputs.primary_process_id];
        if let Some(crankcase) = inputs.crankcase_process_id {
            primary_and_crankcase.push(crankcase);
        }

        // Start from the input minus the consumed EC (deleted unconditionally)
        // and the NonECPM of the iteration's processes (deleted before the
        // adjusted NonECPM is re-inserted).
        let mut mwo: Vec<EmissionRow> = inputs
            .worker_output
            .iter()
            .filter(|row| {
                if row.pollutant_id == EC_POLLUTANT {
                    return false;
                }
                !(row.pollutant_id == NON_EC_PM_POLLUTANT
                    && primary_and_crankcase.contains(&row.process_id))
            })
            .copied()
            .collect();

        // Section MakePM2.5Total — sum EC, residue, sulfate and water into 110
        // for the requested processes. An empty list disables the section.
        if !inputs.pm25_total_process_ids.is_empty() {
            mwo.extend(sum_to_pollutant(
                &spm_output2,
                &PM25_TOTAL_INPUTS,
                |process| inputs.pm25_total_process_ids.contains(&process),
                PM25_TOTAL_POLLUTANT,
            ));
        }

        // Copy EC, sulfate, water and residue verbatim to the output.
        for row in &spm_output2 {
            if COPIED_SPECIES.contains(&row.pollutant_id) {
                mwo.push(*row);
            }
        }

        // Re-sum the adjusted residue, sulfate and water into NonECPM (118) for
        // the iteration's processes.
        mwo.extend(sum_to_pollutant(
            &spm_output2,
            &NON_EC_PM_INPUTS,
            |process| primary_and_crankcase.contains(&process),
            NON_EC_PM_POLLUTANT,
        ));

        // Speciate the spmOutput2 species into the trace metals/ions and
        // organic carbon.
        mwo.extend(speciate(&spm_output2, &inputs.pm_speciation));

        // Total Organic Matter (123) = organic carbon (111) + NCOM (122),
        // read from the output as it stands after speciation.
        let tom = sum_to_pollutant(&mwo, &TOM_INPUTS, |_| true, TOM_POLLUTANT);
        mwo.extend(tom);

        // NonECNonSO4NonOM PM (124) = residue (120) with its organic matter
        // removed.
        let non_om = compute_non_ec_non_so4_non_om(&mwo, &inputs.pm_speciation);
        mwo.extend(non_om);

        // Drop the internal residue (120) unconditionally; drop TOM (123) and
        // NonECNonSO4NonOM (124) unless the RunSpec requested them.
        mwo.retain(|row| {
            if row.pollutant_id == NON_EC_NON_SO4_PM_POLLUTANT {
                return false;
            }
            if row.pollutant_id == TOM_POLLUTANT
                || row.pollutant_id == NON_EC_NON_SO4_NON_OM_POLLUTANT
            {
                return inputs
                    .output_pol_processes
                    .contains(&(row.pollutant_id * 100 + row.process_id));
            }
            true
        });

        mwo.sort_unstable_by_key(EmissionRow::dimension_key);
        mwo
    }
}

/// Port of the SQL's `oneCountyYearSulfateFractions` "Extract Data" aggregation
/// — the headline fuel-sulfur sulfate-fraction algorithm.
///
/// For each `sulfateFractions` row and each run-spec model year inside its
/// window, the market-share-weighted adjusted sulfate and water fractions are
/// accumulated over the fuel formulations of each month: every `FuelSupply`
/// row whose formulation's subtype maps to the row's fuel type contributes
/// `marketShare × fraction × adjustment` to the `(process, fuelType,
/// sourceType, month, modelYear)` cell, where the adjustment scales the base
/// fraction by how far the formulation's sulfur level departs from the base
/// fuel's. The unadjusted base fractions are recorded verbatim for the residue
/// split.
///
/// Every join is an `INNER JOIN`; a `FuelSupply` row whose formulation,
/// subtype or month group is missing is skipped, contributing nothing.
fn compute_sulfate_fractions(
    inputs: &SulfatePmInputs,
) -> FxHashMap<SulfateFractionKey, SulfateFractionCell> {
    let formulation: FxHashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
    let subtype: FxHashMap<i32, &FuelSubtypeRow> = inputs
        .fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst))
        .collect();
    // monthGroupID → the calendar months it covers.
    let mut months_of_group: FxHashMap<i32, Vec<i32>> = FxHashMap::default();
    for month in &inputs.month_of_any_year {
        months_of_group
            .entry(month.month_group_id)
            .or_default()
            .push(month.month_id);
    }

    let mut cells: FxHashMap<SulfateFractionKey, SulfateFractionCell> = FxHashMap::default();
    for sf in &inputs.sulfate_fractions {
        for &model_year in &inputs.run_spec_model_years {
            // sf.minModelYearID <= mya.modelYearID <= sf.maxModelYearID.
            if model_year < sf.min_model_year_id || model_year > sf.max_model_year_id {
                continue;
            }
            for fs in &inputs.fuel_supply {
                // INNER JOIN FuelFormulation USING (fuelFormulationID).
                let Some(ff) = formulation.get(&fs.fuel_formulation_id) else {
                    continue;
                };
                // INNER JOIN FuelSubtype USING (fuelSubtypeID), with the
                // subtype's fuel type pinned to this sulfateFractions row's.
                let Some(fst) = subtype.get(&ff.fuel_subtype_id) else {
                    continue;
                };
                if fst.fuel_type_id != sf.fuel_type_id {
                    continue;
                }
                // INNER JOIN MonthOfAnyYear ON monthGroupID.
                let Some(months) = months_of_group.get(&fs.month_group_id) else {
                    continue;
                };
                // adjustment = 1 + BaseFuelSulfateFraction
                //              × (coalesce(sulfurLevel, 0) / BaseFuelSulfurLevel − 1).
                let sulfur_level = ff.sulfur_level.unwrap_or(0.0);
                let adjustment = 1.0
                    + sf.base_fuel_sulfate_fraction
                        * ((sulfur_level / sf.base_fuel_sulfur_level) - 1.0);
                for &month_id in months {
                    let cell = cells
                        .entry((
                            sf.process_id,
                            sf.fuel_type_id,
                            sf.source_type_id,
                            month_id,
                            model_year,
                        ))
                        .or_default();
                    cell.sulfate_fraction +=
                        fs.market_share * sf.sulfate_non_ec_pm_fraction * adjustment;
                    cell.h2o_fraction += fs.market_share * sf.h2o_non_ec_pm_fraction * adjustment;
                    cell.unadjusted_sulfate_fraction = sf.sulfate_non_ec_pm_fraction;
                    cell.unadjusted_h2o_fraction = sf.h2o_non_ec_pm_fraction;
                }
            }
        }
    }
    cells
}

/// Index the pre-aggregated general fuel-effect ratios by the seven id columns
/// the SQL's `UPDATE` joins on.
/// Index the crankcase split fractions by `(pollutantID, fuelTypeID,
/// sourceTypeID, regClassID)` — the SQL's crankcase-split join key. The
/// model-year window is left on each row; a cell may carry several rows (the
/// primary and crankcase process, and overlapping model-year windows).
fn index_crankcase_split(
    rows: &[CrankcaseSplitRow],
) -> FxHashMap<(i32, i32, i32, i32), Vec<&CrankcaseSplitRow>> {
    let mut index: FxHashMap<(i32, i32, i32, i32), Vec<&CrankcaseSplitRow>> = FxHashMap::default();
    for row in rows {
        index
            .entry((
                row.pollutant_id,
                row.fuel_type_id,
                row.source_type_id,
                row.reg_class_id,
            ))
            .or_default()
            .push(row);
    }
    index
}

/// Sum the rows of `input_pollutants` (whose process passes `keep_process`)
/// into one `output_pollutant` row per output dimension — the port of the SQL's
/// `INSERT … SELECT … SUM(…) GROUP BY` steps (`MakePM2.5Total`, the NonECPM
/// re-sum and Total Organic Matter).
///
/// The `GROUP BY` is the 14 non-pollutant dimension columns; the summed row
/// carries them from the first contributing row (they are identical within a
/// group) with `pollutantID` stamped to `output_pollutant`.
fn sum_to_pollutant(
    rows: &[EmissionRow],
    input_pollutants: &[i32],
    keep_process: impl Fn(i32) -> bool,
    output_pollutant: i32,
) -> Vec<EmissionRow> {
    let mut groups: FxHashMap<[i32; 14], EmissionRow> = FxHashMap::default();
    for row in rows {
        if !input_pollutants.contains(&row.pollutant_id) || !keep_process(row.process_id) {
            continue;
        }
        let entry = groups
            .entry(row.non_pollutant_key())
            .or_insert(EmissionRow {
                pollutant_id: output_pollutant,
                emission_quant: 0.0,
                emission_rate: 0.0,
                ..*row
            });
        entry.emission_quant += row.emission_quant;
        entry.emission_rate += row.emission_rate;
    }
    groups.into_values().collect()
}

/// Speciate each row into its output species — the port of the SQL's
/// `INSERT … SELECT … FROM spmOutput2 JOIN PMSpeciation` step.
///
/// Each row joins every `PMSpeciation` row matching its `(process,
/// pollutant = inputPollutantID, source, fuel)` with the model year inside the
/// speciation window, emitting one `outputPollutantID` row whose emission is
/// scaled by `pmSpeciationFraction`.
fn speciate(rows: &[EmissionRow], pm_speciation: &[PmSpeciationRow]) -> Vec<EmissionRow> {
    let mut index: FxHashMap<(i32, i32, i32, i32), Vec<&PmSpeciationRow>> = FxHashMap::default();
    for ps in pm_speciation {
        index
            .entry((
                ps.process_id,
                ps.input_pollutant_id,
                ps.source_type_id,
                ps.fuel_type_id,
            ))
            .or_default()
            .push(ps);
    }

    let mut out: Vec<EmissionRow> = Vec::new();
    for row in rows {
        let Some(specs) = index.get(&(
            row.process_id,
            row.pollutant_id,
            row.source_type_id,
            row.fuel_type_id,
        )) else {
            continue;
        };
        for ps in specs {
            if row.model_year_id < ps.min_model_year_id || row.model_year_id > ps.max_model_year_id
            {
                continue;
            }
            out.push(EmissionRow {
                pollutant_id: ps.output_pollutant_id,
                emission_quant: row.emission_quant * ps.pm_speciation_fraction,
                emission_rate: row.emission_rate * ps.pm_speciation_fraction,
                ..*row
            });
        }
    }
    out
}

/// Compute NonECNonSO4NonOM PM (124) — the port of the SQL's final
/// `INSERT … SELECT` step.
///
/// `ratio124` for a `(process, inputPollutant, source, fuel, model-year range)`
/// cell is `1 − Σ pmSpeciationFraction` over the organic-carbon (111) and NCOM
/// (122) speciation rows of that cell. Each residue (120) row in `mwo` is
/// matched to every such cell on `(process, source, fuel)` with the model year
/// inside the window, and contributes `emission × ratio124` to the 124 row of
/// its output dimension. See the [module documentation](self) for the
/// single-match assumption this relies on.
fn compute_non_ec_non_so4_non_om(
    mwo: &[EmissionRow],
    pm_speciation: &[PmSpeciationRow],
) -> Vec<EmissionRow> {
    // Σ pmSpeciationFraction over the organic-matter species, keyed by the
    // PMSpeciation sub-aggregate's GROUP BY.
    let mut fraction_sums: FxHashMap<(i32, i32, i32, i32, i32, i32), f64> = FxHashMap::default();
    for ps in pm_speciation {
        if ps.output_pollutant_id != ORGANIC_CARBON_POLLUTANT
            && ps.output_pollutant_id != NCOM_POLLUTANT
        {
            continue;
        }
        *fraction_sums
            .entry((
                ps.process_id,
                ps.input_pollutant_id,
                ps.source_type_id,
                ps.fuel_type_id,
                ps.min_model_year_id,
                ps.max_model_year_id,
            ))
            .or_insert(0.0) += ps.pm_speciation_fraction;
    }

    let mut groups: FxHashMap<[i32; 14], EmissionRow> = FxHashMap::default();
    for row in mwo {
        if row.pollutant_id != NON_EC_NON_SO4_PM_POLLUTANT {
            continue;
        }
        for (&(process, _input, source, fuel, min_my, max_my), &fraction_sum) in &fraction_sums {
            if process != row.process_id || source != row.source_type_id || fuel != row.fuel_type_id
            {
                continue;
            }
            if row.model_year_id < min_my || row.model_year_id > max_my {
                continue;
            }
            let ratio124 = 1.0 - fraction_sum;
            let entry = groups
                .entry(row.non_pollutant_key())
                .or_insert(EmissionRow {
                    pollutant_id: NON_EC_NON_SO4_NON_OM_POLLUTANT,
                    emission_quant: 0.0,
                    emission_rate: 0.0,
                    ..*row
                });
            entry.emission_quant += row.emission_quant * ratio124;
            entry.emission_rate += row.emission_rate * ratio124;
        }
    }
    groups.into_values().collect()
}

/// `(pollutant, process)` registration helper — keeps the [`REGISTRATIONS`]
/// builder readable.
const fn reg(pollutant: u16, process: u16) -> PollutantProcessAssociation {
    PollutantProcessAssociation {
        pollutant_id: PollutantId(pollutant),
        process_id: ProcessId(process),
    }
}

/// The 19 PM-species pollutants `SulfatePMCalculator` registers — the 11 trace
/// metals/ions plus the eight PM2.5 composite/component pollutants. The
/// canonical source is the `Registration` directives for `SulfatePMCalculator`
/// in `CalculatorInfo.txt` at the MOVES source pin.
const REGISTERED_POLLUTANTS: [u16; 19] = [
    35, 36, 51, 52, 53, 54, 55, 56, 57, 58, 59, 110, 111, 112, 115, 118, 119, 121, 122,
];

/// The 7 exhaust processes `SulfatePMCalculator` registers for — running (1),
/// start (2), the three crankcase processes (15/16/17), extended idle (90) and
/// auxiliary power (91).
const REGISTERED_PROCESSES: [u16; 7] = [1, 2, 15, 16, 17, 90, 91];

/// The flattened registration count — 19 pollutants × 7 processes = 133, the
/// number of `Registration` directives recorded for `SulfatePMCalculator` in
/// `CalculatorInfo.txt`.
const REGISTRATION_COUNT: usize = REGISTERED_POLLUTANTS.len() * REGISTERED_PROCESSES.len();

/// The 133 `(pollutant, process)` pairs `SulfatePMCalculator` registers — the
/// cross product of [`REGISTERED_POLLUTANTS`] and [`REGISTERED_PROCESSES`],
/// expanded so [`Calculator::registrations`] can hand back one contiguous
/// slice.
static REGISTRATIONS: [PollutantProcessAssociation; REGISTRATION_COUNT] = {
    let mut regs = [reg(0, 0); REGISTRATION_COUNT];
    let mut idx = 0;
    let mut pi = 0;
    while pi < REGISTERED_POLLUTANTS.len() {
        let mut qi = 0;
        while qi < REGISTERED_PROCESSES.len() {
            regs[idx] = reg(REGISTERED_POLLUTANTS[pi], REGISTERED_PROCESSES[qi]);
            idx += 1;
            qi += 1;
        }
        pi += 1;
    }
    regs
};

/// `SulfatePMCalculator` is a chained calculator — it declares no master-loop
/// subscription of its own.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The upstream calculator `SulfatePMCalculator` chains off —
/// `BaseRateCalculator`, which produces the EC (112) and NonECPM (118) records
/// the speciation consumes. `CalculatorInfo.txt` records
/// `Chain SulfatePMCalculator BaseRateCalculator`.
static UPSTREAM: &[&str] = &["BaseRateCalculator"];

/// Default-DB tables the speciation consumes — the tables
/// `SulfatePMCalculator.sql` reads. `MOVESWorkerOutput` carries the upstream
/// EC / NonECPM rows; the rest feed the sulfate-fraction, fuel-effect,
/// crankcase-split and speciation steps. The SQL also joins the `RunSpec*`
/// filter tables, which only narrow the extract and do not feed the algorithm,
/// so they are not listed.
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "PMSpeciation",
    "PollutantProcessAssoc",
    "RunSpecModelYear",
    "crankcaseEmissionRatio",
    "generalFuelRatio",
    "sulfateFractions",
];

// ===========================================================================
// Data-plane wiring — TableRow impls and build_inputs
// ===========================================================================

fn row_err(table: &'static str, row: usize, col: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: col.into(),
        message: msg,
    }
}

/// Minimal `RunSpecModelYear` row — only `modelYearID` is needed.
struct RunSpecModelYearRow {
    model_year_id: i32,
}

impl TableRow for RunSpecModelYearRow {
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
            vec![Series::new(
                "modelYearID".into(),
                rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecModelYear";
        let col = df
            .column("modelYearID")
            .map_err(|e| row_err(t, 0, "modelYearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "modelYearID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(RunSpecModelYearRow {
                    model_year_id: col
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "modelYearID", "null value".into()))?,
                })
            })
            .collect()
    }
}

/// Minimal `PollutantProcessAssoc` row — `polProcessID`, `pollutantID`, `processID`.
struct PollutantProcessAssocRow {
    pol_process_id: i32,
    pollutant_id: i32,
    process_id: i32,
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
        let pol_process = get_i32("polProcessID")?;
        let process = get_i32("processID")?;
        let pollutant = get_i32("pollutantID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pol_process.get(i).ok_or_else(|| null("polProcessID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
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
        let month_group = get_i32("monthGroupID")?;
        let formulation = get_i32("fuelFormulationID")?;
        let market_share = df
            .column("marketShare")
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    month_group_id: month_group.get(i).ok_or_else(|| null("monthGroupID"))?,
                    fuel_formulation_id: formulation
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
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
            ("fuelSubtypeID".into(), DataType::Int32),
            ("sulfurLevel".into(), DataType::Float64),
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
                    "sulfurLevel".into(),
                    rows.iter()
                        .map(|r| r.sulfur_level)
                        .collect::<Vec<Option<f64>>>(),
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
        let formulation = get_i32("fuelFormulationID")?;
        let subtype = get_i32("fuelSubtypeID")?;
        let sulfur = df
            .column("sulfurLevel")
            .map_err(|e| row_err(t, 0, "sulfurLevel", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sulfurLevel", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: formulation
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_subtype_id: subtype.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    sulfur_level: sulfur.get(i), // Option<f64> — NULL → None
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
        let subtype = get_i32("fuelSubtypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubtypeRow {
                    fuel_subtype_id: subtype.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SulfateFractionsRow {
    fn table_name() -> &'static str {
        "sulfateFractions"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("SulfatenonECPMFraction".into(), DataType::Float64),
            ("H2ONonECPMFraction".into(), DataType::Float64),
            ("BaseFuelSulfateFraction".into(), DataType::Float64),
            ("BaseFuelSulfurLevel".into(), DataType::Float64),
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
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
                    "SulfatenonECPMFraction".into(),
                    rows.iter()
                        .map(|r| r.sulfate_non_ec_pm_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "H2ONonECPMFraction".into(),
                    rows.iter()
                        .map(|r| r.h2o_non_ec_pm_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "BaseFuelSulfateFraction".into(),
                    rows.iter()
                        .map(|r| r.base_fuel_sulfate_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "BaseFuelSulfurLevel".into(),
                    rows.iter()
                        .map(|r| r.base_fuel_sulfur_level)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sulfateFractions";
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
        let process = get_i32("processID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let source_type = get_i32("sourceTypeID")?;
        let min_my = get_i32("minModelYearID")?;
        let max_my = get_i32("maxModelYearID")?;
        let sulfate = get_f64("SulfatenonECPMFraction")?;
        let h2o = get_f64("H2ONonECPMFraction")?;
        let base_sulfate = get_f64("BaseFuelSulfateFraction")?;
        let base_sulfur = get_f64("BaseFuelSulfurLevel")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SulfateFractionsRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    source_type_id: source_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    min_model_year_id: min_my.get(i).ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_my.get(i).ok_or_else(|| null("maxModelYearID"))?,
                    sulfate_non_ec_pm_fraction: sulfate
                        .get(i)
                        .ok_or_else(|| null("SulfatenonECPMFraction"))?,
                    h2o_non_ec_pm_fraction: h2o.get(i).ok_or_else(|| null("H2ONonECPMFraction"))?,
                    base_fuel_sulfate_fraction: base_sulfate
                        .get(i)
                        .ok_or_else(|| null("BaseFuelSulfateFraction"))?,
                    base_fuel_sulfur_level: base_sulfur
                        .get(i)
                        .ok_or_else(|| null("BaseFuelSulfurLevel"))?,
                })
            })
            .collect()
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
        let month_group = get_i32("monthGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MonthGroupRow {
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    month_group_id: month_group.get(i).ok_or_else(|| null("monthGroupID"))?,
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
            ("fuelTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("minAgeID".into(), DataType::Int32),
            ("maxAgeID".into(), DataType::Int32),
            ("fuelEffectRatio".into(), DataType::Float64),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
                    "fuelEffectRatio".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio)
                        .collect::<Vec<f64>>(),
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
        let fuel_type = get_i32("fuelTypeID")?;
        let source_type = get_i32("sourceTypeID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let min_model_year = get_i32("minModelYearID")?;
        let max_model_year = get_i32("maxModelYearID")?;
        let min_age = get_i32("minAgeID")?;
        let max_age = get_i32("maxAgeID")?;
        let ratio = df
            .column("fuelEffectRatio")
            .map_err(|e| row_err(t, 0, "fuelEffectRatio", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "fuelEffectRatio", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GeneralFuelRatioRow {
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    source_type_id: source_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    min_model_year_id: min_model_year
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    min_age_id: min_age.get(i).ok_or_else(|| null("minAgeID"))?,
                    max_age_id: max_age.get(i).ok_or_else(|| null("maxAgeID"))?,
                    fuel_effect_ratio: ratio.get(i).ok_or_else(|| null("fuelEffectRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CrankcaseSplitRow {
    fn table_name() -> &'static str {
        "crankcaseEmissionRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("crankcaseRatio".into(), DataType::Float64),
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
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
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
                    "crankcaseRatio".into(),
                    rows.iter().map(|r| r.crankcase_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "crankcaseEmissionRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let process = get_i32("processID")?;
        let pollutant = get_i32("pollutantID")?;
        let source_type = get_i32("sourceTypeID")?;
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let min_my = get_i32("minModelYearID")?;
        let max_my = get_i32("maxModelYearID")?;
        let ratio = df
            .column("crankcaseRatio")
            .map_err(|e| row_err(t, 0, "crankcaseRatio", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "crankcaseRatio", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CrankcaseSplitRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    source_type_id: source_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    min_model_year_id: min_my.get(i).ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_my.get(i).ok_or_else(|| null("maxModelYearID"))?,
                    crankcase_ratio: ratio.get(i).ok_or_else(|| null("crankcaseRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PmSpeciationRow {
    fn table_name() -> &'static str {
        "PMSpeciation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("inputPollutantID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("outputPollutantID".into(), DataType::Int32),
            ("pmSpeciationFraction".into(), DataType::Float64),
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
                    "inputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.input_pollutant_id)
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
                    "outputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.output_pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pmSpeciationFraction".into(),
                    rows.iter()
                        .map(|r| r.pm_speciation_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PMSpeciation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let process = get_i32("processID")?;
        let input_pol = get_i32("inputPollutantID")?;
        let source_type = get_i32("sourceTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let min_my = get_i32("minModelYearID")?;
        let max_my = get_i32("maxModelYearID")?;
        let output_pol = get_i32("outputPollutantID")?;
        let fraction = df
            .column("pmSpeciationFraction")
            .map_err(|e| row_err(t, 0, "pmSpeciationFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "pmSpeciationFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PmSpeciationRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    input_pollutant_id: input_pol.get(i).ok_or_else(|| null("inputPollutantID"))?,
                    source_type_id: source_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    min_model_year_id: min_my.get(i).ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_my.get(i).ok_or_else(|| null("maxModelYearID"))?,
                    output_pollutant_id: output_pol
                        .get(i)
                        .ok_or_else(|| null("outputPollutantID"))?,
                    pm_speciation_fraction: fraction
                        .get(i)
                        .ok_or_else(|| null("pmSpeciationFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionRow {
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
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRow {
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
            })
            .collect()
    }
}

/// Derive the crankcase process for a primary exhaust process.
/// Running (1) → 15, Start (2) → 16, Extended Idle (90) → 17, others → None.
fn crankcase_for(primary: i32) -> Option<i32> {
    match primary {
        1 => Some(15),
        2 => Some(16),
        90 => Some(17),
        _ => None,
    }
}

/// Read all `SulfatePMCalculator` input tables from `ctx.tables()`.
fn build_inputs(ctx: &CalculatorContext) -> Result<SulfatePmInputs, Error> {
    let tables = ctx.tables();
    let filter = crate::wiring::position_filter(ctx);
    let primary_process_id = ctx
        .position()
        .process_id
        .map(|p| i32::from(p.0))
        .unwrap_or(0);
    let crankcase_process_id = crankcase_for(primary_process_id);

    let mut pm25_total_process_ids = vec![primary_process_id];
    if let Some(cc) = crankcase_process_id {
        pm25_total_process_ids.push(cc);
    }

    let output_pol_processes: Vec<i32> = tables
        .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?
        .into_iter()
        .map(|r| r.pol_process_id)
        .collect();

    let run_spec_model_years: Vec<i32> = tables
        .iter_typed::<RunSpecModelYearRow>("RunSpecModelYear")?
        .into_iter()
        .map(|r| r.model_year_id)
        .collect();

    Ok(SulfatePmInputs {
        fuel_supply: tables.iter_typed("FuelSupply")?,
        fuel_formulation: tables.iter_typed("FuelFormulation")?,
        fuel_subtype: tables.iter_typed("FuelSubtype")?,
        sulfate_fractions: tables.iter_typed("sulfateFractions")?,
        month_of_any_year: tables.iter_typed("MonthOfAnyYear")?,
        run_spec_model_years,
        general_fuel_ratio: tables.iter_typed("generalFuelRatio")?,
        crankcase_split: tables.iter_typed("crankcaseEmissionRatio")?,
        pm_speciation: tables.iter_typed("PMSpeciation")?,
        worker_output: tables
            .iter_typed::<EmissionRow>("MOVESWorkerOutput")?
            .into_iter()
            .filter(|r| filter.matches(r.year_id, r.county_id, r.process_id))
            .collect(),
        primary_process_id,
        crankcase_process_id,
        pm25_total_process_ids,
        output_pol_processes,
    })
}

impl Calculator for SulfatePMCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `SulfatePMCalculator` is a chained calculator: it does not subscribe to
    /// the MasterLoop directly but fires when its upstream `BaseRateCalculator`
    /// does. `CalculatorInfo.txt` carries no `Subscribe` directive for it.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

    /// `SulfatePMCalculator` chains off `BaseRateCalculator` —
    /// `CalculatorInfo.txt` records `Chain SulfatePMCalculator BaseRateCalculator`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let inputs = build_inputs(ctx)?;
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(SulfatePMCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert `actual` matches `expected` within `f64` slack.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    /// One EC / NonECPM emission row in the minimal scenario's dimension cell:
    /// year 2020, month 1, model year 2018, fuel type 2, source type 21,
    /// reg class 30, process 1 (Running Exhaust).
    fn base_row(pollutant: i32, quant: f64, rate: f64) -> EmissionRow {
        EmissionRow {
            year_id: 2020,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            pollutant_id: pollutant,
            process_id: 1,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 2,
            model_year_id: 2018,
            road_type_id: 4,
            emission_quant: quant,
            emission_rate: rate,
        }
    }

    /// A minimal one-cell scenario with hand-computable results.
    ///
    /// One fuel formulation (sulfur level 30) over one month, one
    /// `sulfateFractions` row (base sulfate 0.2, base water 0.05,
    /// `BaseFuelSulfateFraction` 0.1, `BaseFuelSulfurLevel` 10), so
    /// `adjustment = 1 + 0.1 × (30/10 − 1) = 1.2` and the adjusted fractions
    /// are sulfate 0.24, water 0.06; the residue fraction is
    /// `1 − 0.05 − 0.2 = 0.75`.
    ///
    /// Inputs: EC (112) quant 100, NonECPM (118) quant 1000. Crankcase split is
    /// the identity (primary process, ratio 1). One `PMSpeciation` row
    /// speciates the residue (120) into organic carbon (111) at fraction 0.3.
    fn minimal_inputs() -> SulfatePmInputs {
        SulfatePmInputs {
            fuel_supply: vec![FuelSupplyRow {
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 21,
                sulfur_level: Some(30.0),
            }],
            fuel_subtype: vec![FuelSubtypeRow {
                fuel_subtype_id: 21,
                fuel_type_id: 2,
            }],
            sulfate_fractions: vec![SulfateFractionsRow {
                process_id: 1,
                fuel_type_id: 2,
                source_type_id: 21,
                min_model_year_id: 2000,
                max_model_year_id: 2025,
                sulfate_non_ec_pm_fraction: 0.2,
                h2o_non_ec_pm_fraction: 0.05,
                base_fuel_sulfate_fraction: 0.1,
                base_fuel_sulfur_level: 10.0,
            }],
            month_of_any_year: vec![MonthGroupRow {
                month_id: 1,
                month_group_id: 1,
            }],
            run_spec_model_years: vec![2018],
            general_fuel_ratio: vec![],
            crankcase_split: vec![
                crankcase_identity(EC_POLLUTANT),
                crankcase_identity(SULFATE_POLLUTANT),
                crankcase_identity(H2O_POLLUTANT),
                crankcase_identity(NON_EC_NON_SO4_PM_POLLUTANT),
            ],
            pm_speciation: vec![PmSpeciationRow {
                process_id: 1,
                input_pollutant_id: NON_EC_NON_SO4_PM_POLLUTANT,
                source_type_id: 21,
                fuel_type_id: 2,
                min_model_year_id: 2000,
                max_model_year_id: 2025,
                output_pollutant_id: ORGANIC_CARBON_POLLUTANT,
                pm_speciation_fraction: 0.3,
            }],
            worker_output: vec![
                base_row(EC_POLLUTANT, 100.0, 10.0),
                base_row(NON_EC_PM_POLLUTANT, 1000.0, 50.0),
            ],
            primary_process_id: 1,
            crankcase_process_id: Some(15),
            pm25_total_process_ids: vec![],
            output_pol_processes: vec![123 * 100 + 1, 124 * 100 + 1],
        }
    }

    /// An identity crankcase split — keeps the pollutant in the primary process
    /// (process 1) with ratio 1, so `spmOutput2` equals `spmOutput`.
    fn crankcase_identity(pollutant: i32) -> CrankcaseSplitRow {
        CrankcaseSplitRow {
            process_id: 1,
            pollutant_id: pollutant,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 2,
            min_model_year_id: 2000,
            max_model_year_id: 2025,
            crankcase_ratio: 1.0,
        }
    }

    /// Find the single output row for `pollutant`, asserting there is exactly
    /// one.
    fn row_for(rows: &[EmissionRow], pollutant: i32) -> EmissionRow {
        let matches: Vec<&EmissionRow> = rows
            .iter()
            .filter(|r| r.pollutant_id == pollutant)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one row for pollutant {pollutant}, got {}",
            matches.len(),
        );
        *matches[0]
    }

    #[test]
    fn compute_sulfate_fractions_applies_the_fuel_sulfur_adjustment() {
        // adjustment = 1 + 0.1 × (30/10 − 1) = 1.2;
        // sulfate 0.2 × 1.2 = 0.24, water 0.05 × 1.2 = 0.06.
        let cells = compute_sulfate_fractions(&minimal_inputs());
        let cell = cells.get(&(1, 2, 21, 1, 2018)).expect("cell present");
        assert_close(cell.sulfate_fraction, 0.24);
        assert_close(cell.h2o_fraction, 0.06);
        assert_close(cell.unadjusted_sulfate_fraction, 0.2);
        assert_close(cell.unadjusted_h2o_fraction, 0.05);
    }

    #[test]
    fn compute_sulfate_fractions_coalesces_a_null_sulfur_level_to_zero() {
        // A null sulfur level → adjustment = 1 + 0.1 × (0/10 − 1) = 0.9.
        let mut inputs = minimal_inputs();
        inputs.fuel_formulation[0].sulfur_level = None;
        let cells = compute_sulfate_fractions(&inputs);
        let cell = cells.get(&(1, 2, 21, 1, 2018)).expect("cell present");
        assert_close(cell.sulfate_fraction, 0.2 * 0.9);
        assert_close(cell.h2o_fraction, 0.05 * 0.9);
    }

    #[test]
    fn compute_sulfate_fractions_weights_by_market_share() {
        // Two formulations, shares 0.25 / 0.75, sulfur 30 / 10:
        //   adj30 = 1.2, adj10 = 1 + 0.1 × (10/10 − 1) = 1.0
        //   sulfate = 0.25 × 0.2 × 1.2 + 0.75 × 0.2 × 1.0 = 0.06 + 0.15 = 0.21
        let mut inputs = minimal_inputs();
        inputs.fuel_supply = vec![
            FuelSupplyRow {
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 0.25,
            },
            FuelSupplyRow {
                month_group_id: 1,
                fuel_formulation_id: 101,
                market_share: 0.75,
            },
        ];
        inputs.fuel_formulation.push(FuelFormulationRow {
            fuel_formulation_id: 101,
            fuel_subtype_id: 21,
            sulfur_level: Some(10.0),
        });
        let cells = compute_sulfate_fractions(&inputs);
        let cell = cells.get(&(1, 2, 21, 1, 2018)).expect("cell present");
        assert_close(cell.sulfate_fraction, 0.21);
    }

    #[test]
    fn calculate_minimal_scenario_produces_the_full_species_family() {
        // spmOutput: EC 100; from NonECPM 1000 — 115 = 1000×0.24 = 240,
        // 119 = 1000×0.06 = 60, 120 = 1000×0.75 = 750.
        // 118 re-sum = 240 + 60 + 750 = 1050.
        // 111 = 750 × 0.3 = 225; 123 (TOM) = 225; 124 = 750 × (1 − 0.3) = 525.
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        // EC, sulfate, water, NonECPM, organic carbon, TOM, NonECNonSO4NonOM.
        assert_eq!(rows.len(), 7, "expected seven output rows");
        assert_close(row_for(&rows, EC_POLLUTANT).emission_quant, 100.0);
        assert_close(row_for(&rows, SULFATE_POLLUTANT).emission_quant, 240.0);
        assert_close(row_for(&rows, H2O_POLLUTANT).emission_quant, 60.0);
        assert_close(row_for(&rows, NON_EC_PM_POLLUTANT).emission_quant, 1050.0);
        assert_close(
            row_for(&rows, ORGANIC_CARBON_POLLUTANT).emission_quant,
            225.0,
        );
        assert_close(row_for(&rows, TOM_POLLUTANT).emission_quant, 225.0);
        assert_close(
            row_for(&rows, NON_EC_NON_SO4_NON_OM_POLLUTANT).emission_quant,
            525.0,
        );
    }

    #[test]
    fn calculate_scales_the_emission_rate_alongside_the_quantity() {
        // The minimal NonECPM rate is 50: 115 = 50×0.24 = 12, 119 = 50×0.06 = 3,
        // 120 = 50×0.75 = 37.5; 118 re-sum = 52.5.
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        assert_close(row_for(&rows, EC_POLLUTANT).emission_rate, 10.0);
        assert_close(row_for(&rows, SULFATE_POLLUTANT).emission_rate, 12.0);
        assert_close(row_for(&rows, H2O_POLLUTANT).emission_rate, 3.0);
        assert_close(row_for(&rows, NON_EC_PM_POLLUTANT).emission_rate, 52.5);
    }

    #[test]
    fn calculate_drops_the_internal_residue_pollutant() {
        // Pollutant 120 (NonECNonSO4PM) is always deleted from the output.
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        assert!(rows
            .iter()
            .all(|r| r.pollutant_id != NON_EC_NON_SO4_PM_POLLUTANT));
    }

    #[test]
    fn calculate_clamps_the_residue_fraction_at_zero() {
        // Base fractions that sum above 1 (sulfate 0.7, water 0.5) would give a
        // negative residue fraction; greatest(…, 0) clamps it, so the residue,
        // organic carbon, TOM and NonECNonSO4NonOM are all zero.
        let mut inputs = minimal_inputs();
        inputs.sulfate_fractions[0].sulfate_non_ec_pm_fraction = 0.7;
        inputs.sulfate_fractions[0].h2o_non_ec_pm_fraction = 0.5;
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert_close(row_for(&rows, ORGANIC_CARBON_POLLUTANT).emission_quant, 0.0);
        assert_close(row_for(&rows, TOM_POLLUTANT).emission_quant, 0.0);
        assert_close(
            row_for(&rows, NON_EC_NON_SO4_NON_OM_POLLUTANT).emission_quant,
            0.0,
        );
    }

    #[test]
    fn calculate_applies_the_general_fuel_ratio_to_the_residue_only() {
        // A ratio of 2 for pollutant 120 doubles the residue: 120 → 1500, so
        // 118 re-sum = 240 + 60 + 1500 = 1800, 111 = 1500×0.3 = 450,
        // 124 = 1500×0.7 = 1050. Sulfate (115) and water (119) are untouched.
        // base_row: year=2020, model_year=2018, age=2, fuel=2, source=21.
        let mut inputs = minimal_inputs();
        inputs.general_fuel_ratio = vec![GeneralFuelRatioRow {
            fuel_type_id: 2,
            source_type_id: 21,
            pollutant_id: NON_EC_NON_SO4_PM_POLLUTANT,
            process_id: 1,
            min_model_year_id: 2000,
            max_model_year_id: 2025,
            min_age_id: 0,
            max_age_id: 30,
            fuel_effect_ratio: 2.0,
        }];
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert_close(row_for(&rows, SULFATE_POLLUTANT).emission_quant, 240.0);
        assert_close(row_for(&rows, H2O_POLLUTANT).emission_quant, 60.0);
        assert_close(row_for(&rows, NON_EC_PM_POLLUTANT).emission_quant, 1800.0);
        assert_close(
            row_for(&rows, ORGANIC_CARBON_POLLUTANT).emission_quant,
            450.0,
        );
        assert_close(
            row_for(&rows, NON_EC_NON_SO4_NON_OM_POLLUTANT).emission_quant,
            1050.0,
        );
    }

    #[test]
    fn calculate_leaves_emission_unchanged_when_no_fuel_ratio_matches() {
        // A ratio row that matches every column but the fuel type does not
        // apply — the SQL UPDATE leaves the unmatched row untouched.
        let mut inputs = minimal_inputs();
        inputs.general_fuel_ratio = vec![GeneralFuelRatioRow {
            fuel_type_id: 99, // mismatches base_row fuel_type_id 2
            source_type_id: 21,
            pollutant_id: NON_EC_NON_SO4_PM_POLLUTANT,
            process_id: 1,
            min_model_year_id: 2000,
            max_model_year_id: 2025,
            min_age_id: 0,
            max_age_id: 30,
            fuel_effect_ratio: 2.0,
        }];
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert_close(row_for(&rows, NON_EC_PM_POLLUTANT).emission_quant, 1050.0);
    }

    #[test]
    fn calculate_splits_each_species_across_the_crankcase_process() {
        // Add a crankcase split (process 15, ratio 0.1) for every species. Each
        // primary-process row now also produces a crankcase-process row scaled
        // by 0.1, so every species appears for processes 1 and 15.
        let mut inputs = minimal_inputs();
        for pollutant in COPIED_SPECIES {
            inputs.crankcase_split.push(CrankcaseSplitRow {
                process_id: 15,
                pollutant_id: pollutant,
                source_type_id: 21,
                reg_class_id: 30,
                fuel_type_id: 2,
                min_model_year_id: 2000,
                max_model_year_id: 2025,
                crankcase_ratio: 0.1,
            });
        }
        let rows = SulfatePMCalculator.calculate(&inputs);
        let sulfate_15: Vec<&EmissionRow> = rows
            .iter()
            .filter(|r| r.pollutant_id == SULFATE_POLLUTANT && r.process_id == 15)
            .collect();
        assert_eq!(sulfate_15.len(), 1);
        // 240 (primary sulfate) × 0.1.
        assert_close(sulfate_15[0].emission_quant, 24.0);
        // The primary-process sulfate row survives unchanged.
        let sulfate_1: Vec<&EmissionRow> = rows
            .iter()
            .filter(|r| r.pollutant_id == SULFATE_POLLUTANT && r.process_id == 1)
            .collect();
        assert_eq!(sulfate_1.len(), 1);
        assert_close(sulfate_1[0].emission_quant, 240.0);
    }

    #[test]
    fn calculate_makes_pm25_total_when_the_section_is_enabled() {
        // With process 1 in the PM2.5-total list, pollutant 110 is the sum of
        // EC 100, sulfate 240, water 60 and residue 750 = 1150.
        let mut inputs = minimal_inputs();
        inputs.pm25_total_process_ids = vec![1];
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert_close(row_for(&rows, PM25_TOTAL_POLLUTANT).emission_quant, 1150.0);
    }

    #[test]
    fn calculate_omits_pm25_total_when_the_section_is_disabled() {
        // The minimal scenario leaves the PM2.5-total list empty.
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        assert!(rows.iter().all(|r| r.pollutant_id != PM25_TOTAL_POLLUTANT));
    }

    #[test]
    fn calculate_keeps_tom_and_residue_species_only_when_requested() {
        // With an empty output-pol-process list, TOM (123) and NonECNonSO4NonOM
        // (124) are dropped; the other five species remain.
        let mut inputs = minimal_inputs();
        inputs.output_pol_processes = vec![];
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert!(rows.iter().all(|r| r.pollutant_id != TOM_POLLUTANT));
        assert!(rows
            .iter()
            .all(|r| r.pollutant_id != NON_EC_NON_SO4_NON_OM_POLLUTANT));
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn calculate_speciates_into_multiple_output_species() {
        // A second PMSpeciation row makes NCOM (122) at fraction 0.1 from the
        // residue; TOM (123) is then organic carbon 225 + NCOM 75 = 300.
        let mut inputs = minimal_inputs();
        inputs.pm_speciation.push(PmSpeciationRow {
            process_id: 1,
            input_pollutant_id: NON_EC_NON_SO4_PM_POLLUTANT,
            source_type_id: 21,
            fuel_type_id: 2,
            min_model_year_id: 2000,
            max_model_year_id: 2025,
            output_pollutant_id: NCOM_POLLUTANT,
            pm_speciation_fraction: 0.1,
        });
        let rows = SulfatePMCalculator.calculate(&inputs);
        assert_close(row_for(&rows, NCOM_POLLUTANT).emission_quant, 75.0);
        assert_close(row_for(&rows, TOM_POLLUTANT).emission_quant, 300.0);
        // ratio124 = 1 − (0.3 + 0.1) = 0.6, so 124 = 750 × 0.6 = 450.
        assert_close(
            row_for(&rows, NON_EC_NON_SO4_NON_OM_POLLUTANT).emission_quant,
            450.0,
        );
    }

    #[test]
    fn calculate_passes_unrelated_pollutants_through() {
        // A CO (pollutant 2) row the calculator does not touch survives intact.
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(base_row(2, 999.0, 9.0));
        let rows = SulfatePMCalculator.calculate(&inputs);
        let co = row_for(&rows, 2);
        assert_close(co.emission_quant, 999.0);
        assert_close(co.emission_rate, 9.0);
    }

    #[test]
    fn calculate_consumes_the_input_ec_and_non_ec_pm_rows() {
        // The output's EC (112) is the adjusted/crankcase-split EC, not the raw
        // input row, and the input NonECPM (118) is replaced by the re-sum.
        // Both still come back exactly once.
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        assert_eq!(
            rows.iter()
                .filter(|r| r.pollutant_id == EC_POLLUTANT)
                .count(),
            1,
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.pollutant_id == NON_EC_PM_POLLUTANT)
                .count(),
            1,
        );
    }

    #[test]
    fn calculate_drops_a_non_ec_pm_row_without_a_fraction_cell() {
        // A NonECPM row whose model year is outside every sulfateFractions
        // window resolves no cell, so it produces no sulfate/water/residue.
        let mut inputs = minimal_inputs();
        inputs.run_spec_model_years = vec![1990];
        inputs.worker_output = vec![
            base_row(EC_POLLUTANT, 100.0, 10.0),
            EmissionRow {
                model_year_id: 1990,
                ..base_row(NON_EC_PM_POLLUTANT, 1000.0, 50.0)
            },
        ];
        let rows = SulfatePMCalculator.calculate(&inputs);
        // Only EC survives — no sulfate, water, residue or downstream species.
        assert!(rows.iter().all(|r| r.pollutant_id == EC_POLLUTANT));
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        let rows = SulfatePMCalculator.calculate(&minimal_inputs());
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        let inputs = SulfatePmInputs {
            primary_process_id: 1,
            ..SulfatePmInputs::default()
        };
        assert!(SulfatePMCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculator_name_matches_the_module() {
        assert_eq!(SulfatePMCalculator.name(), "SulfatePMCalculator");
        assert_eq!(SulfatePMCalculator::NAME, "SulfatePMCalculator");
    }

    #[test]
    fn calculator_is_a_chained_calculator_with_no_subscriptions() {
        assert!(SulfatePMCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_are_the_133_calculator_info_directives() {
        // 19 PM-species pollutants × 7 exhaust processes = 133 Registration
        // directives in CalculatorInfo.txt.
        let regs = SulfatePMCalculator.registrations();
        assert_eq!(regs.len(), 133);
        // Every registered pollutant appears for exactly the 7 processes.
        for &pollutant in &REGISTERED_POLLUTANTS {
            let mut procs: Vec<u16> = regs
                .iter()
                .filter(|r| r.pollutant_id == PollutantId(pollutant))
                .map(|r| r.process_id.0)
                .collect();
            procs.sort_unstable();
            assert_eq!(procs, vec![1, 2, 15, 16, 17, 90, 91]);
        }
        // No pair is registered twice.
        let mut pairs: Vec<(u16, u16)> = regs
            .iter()
            .map(|r| (r.pollutant_id.0, r.process_id.0))
            .collect();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(pairs.len(), 133);
    }

    #[test]
    fn calculator_chains_off_base_rate_calculator() {
        assert_eq!(SulfatePMCalculator.upstream(), &["BaseRateCalculator"]);
    }

    #[test]
    fn calculator_declares_input_tables() {
        let tables = SulfatePMCalculator.input_tables();
        for expected in [
            "FuelFormulation",
            "FuelSubtype",
            "FuelSupply",
            "MOVESWorkerOutput",
            "MonthOfAnyYear",
            "PMSpeciation",
            "PollutantProcessAssoc",
            "RunSpecModelYear",
            "crankcaseEmissionRatio",
            "generalFuelRatio",
            "sulfateFractions",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        let inputs = minimal_inputs();
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubtypeRow::into_dataframe(inputs.fuel_subtype).unwrap(),
        );
        store.insert(
            "sulfateFractions",
            SulfateFractionsRow::into_dataframe(inputs.sulfate_fractions).unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            MonthGroupRow::into_dataframe(inputs.month_of_any_year).unwrap(),
        );
        store.insert(
            "RunSpecModelYear",
            RunSpecModelYearRow::into_dataframe(
                inputs
                    .run_spec_model_years
                    .iter()
                    .map(|&y| RunSpecModelYearRow { model_year_id: y })
                    .collect(),
            )
            .unwrap(),
        );
        store.insert(
            "generalFuelRatio",
            GeneralFuelRatioRow::into_dataframe(inputs.general_fuel_ratio).unwrap(),
        );
        store.insert(
            "crankcaseEmissionRatio",
            CrankcaseSplitRow::into_dataframe(inputs.crankcase_split).unwrap(),
        );
        store.insert(
            "PMSpeciation",
            PmSpeciationRow::into_dataframe(inputs.pm_speciation).unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            EmissionRow::into_dataframe(inputs.worker_output).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(vec![
                PollutantProcessAssocRow {
                    pol_process_id: TOM_POLLUTANT * 100 + 1,
                    pollutant_id: TOM_POLLUTANT,
                    process_id: 1,
                },
                PollutantProcessAssocRow {
                    pol_process_id: NON_EC_NON_SO4_NON_OM_POLLUTANT * 100 + 1,
                    pollutant_id: NON_EC_NON_SO4_NON_OM_POLLUTANT,
                    process_id: 1,
                },
            ])
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = SulfatePMCalculator.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            7,
            "minimal inputs produce exactly seven species rows"
        );
        let sulfate_quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .iter()
            .zip(df.column("pollutantID").unwrap().i32().unwrap().iter())
            .find_map(|(q, p)| {
                if p == Some(SULFATE_POLLUTANT) {
                    q
                } else {
                    None
                }
            })
            .expect("sulfate row present");
        assert!(
            (sulfate_quant - 240.0).abs() < 1e-9,
            "sulfate emissionQuant {sulfate_quant} != 240.0"
        );
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "SulfatePMCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        let calc: Box<dyn Calculator> = Box::new(SulfatePMCalculator);
        assert_eq!(calc.name(), "SulfatePMCalculator");
    }
}
