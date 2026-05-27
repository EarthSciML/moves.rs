//! Port of `LiquidLeakingCalculator.java` and
//! `database/LiquidLeakingCalculator.sql` — migration plan Phase 3, Task 61.
//!
//! `LiquidLeakingCalculator` computes **Total Gaseous Hydrocarbon (THC)**
//! emissions for the **Evap Fuel Leaks** process — the raw fuel that weeps
//! past worn seals, gaskets and fittings of a parked or operating vehicle and
//! evaporates. It is one of the MOVES evaporative-emission calculators.
//!
//! # Direct subscriber
//!
//! `LiquidLeakingCalculator` extends `GenericCalculatorBase` and subscribes to
//! the MasterLoop directly: `CalculatorInfo.txt` records a single `Subscribe`
//! directive — the Evap Fuel Leaks process (id 13) at `MONTH` granularity,
//! `EMISSION_CALCULATOR` priority. The one `Chain` directive that names the
//! calculator, `Chain → HCSpeciationCalculator → LiquidLeakingCalculator`,
//! has it as the *inModule*: `HCSpeciationCalculator` chains *off*
//! `LiquidLeakingCalculator`, so the dependency edge points the other way.
//! `LiquidLeakingCalculator` itself has no upstream, and
//! [`upstream`](Calculator::upstream) keeps the trait default (empty) — the
//! `DistanceCalculator` direct-subscriber shape, not the `SO2Calculator`
//! chained shape.
//!
//! # What it computes
//!
//! Liquid fuel leaks are a base rate scaled by the vehicle activity and by the
//! share of that activity spent in each operating mode:
//!
//! ```text
//! emissionQuant = weightedMeanBaseRate × sourceHours × opModeFraction
//! ```
//!
//! then, where an inspection-and-maintenance (I/M) program covers the cell,
//!
//! ```text
//! emissionQuant = max(emissionQuantIM × IMAdjustFract
//!                     + emissionQuant × (1 − IMAdjustFract), 0)
//! ```
//!
//! * `weightedMeanBaseRate` — the source-bin-activity-weighted mean leak rate
//!   for the cell, with `weightedMeanBaseRateIM` its I/M-program counterpart.
//! * `sourceHours` — the hours of vehicle activity in the cell.
//! * `opModeFraction` — the fraction of that activity in the cell's operating
//!   mode.
//! * `IMAdjustFract` — the I/M adjustment fraction: a value of `f` blends the
//!   I/M rate in at weight `f` and the non-I/M rate at weight `1 − f`.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! [`LiquidLeakingCalculator::calculate`] ports
//! `LiquidLeakingCalculator.sql`'s "Processing" section: two working tables
//! the SQL labels LL-1 and LL-8, an LL-9 insert, and an I/M-adjustment
//! `UPDATE`. The port folds them into two index maps and one join loop:
//!
//! | SQL working table / step | This port |
//! |--------------------------|-----------|
//! | `IMCoverageMergedUngrouped` (LL-1) | `(process, pollutant, modelYear, fuelType, sourceType) → IMAdjustFract` |
//! | `WeightedMeanBaseRate` (LL-8) | `(polProcess, sourceType, regClass, fuelType, month, hourDay, modelYear, opMode) → (weightedMeanBaseRate, weightedMeanBaseRateIM)` |
//! | `MOVESWorkerOutput` (LL-9 + Apply I/M) | the returned `Vec<`[`LiquidLeakingEmissionRow`]`>` |
//!
//! LL-1 disaggregates each `IMCoverage` program record across the individual
//! model years its range covers, summing `IMFactor × complianceFactor × 0.01`.
//! LL-8 sums `sourceBinActivityFraction × meanBaseRate` (and the `…IM`
//! variant) over the source bins of each dimension cell, then cross-joins the
//! run's months and hour/day combinations. LL-9 multiplies the weighted rate
//! by `sourceHours` and `opModeFraction`; the trailing `UPDATE` blends in the
//! I/M rate where an `IMCoverageMergedUngrouped` cell matches the row.
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key
//! is dropped; the port reproduces that with map lookups that skip on a miss.
//! The I/M `UPDATE` is a multi-table `UPDATE`, not a join — a row with no
//! matching `IMCoverageMergedUngrouped` cell keeps its value unchanged; the
//! port folds it into the LL-9 row loop, which is equivalent because
//! `IMCoverageMergedUngrouped` carries at most one cell per join key (it is
//! the LL-1 `GROUP BY` result) so each output row is adjusted at most once.
//!
//! # `WithRegClassID` only
//!
//! LL-8 has a `WithRegClassID` and a `NoRegClassID` variant — the former
//! carries `SourceBin.regClassID` as an output dimension, the latter writes a
//! literal `0`. `BundleUtilities.prepareCountyDataWithRunSpec`
//! unconditionally adds `"WithRegClassID"` to the enabled SQL sections, so
//! `NoRegClassID` is dead in current MOVES. This port implements
//! `WithRegClassID` alone; `LiquidLeakingCalculator.sql` retains `NoRegClassID`
//! as reference. This matches the `ActivityCalculator` port (Task 71).
//!
//! # Scope of this port
//!
//! [`calculate`](LiquidLeakingCalculator::calculate) is the SQL "Processing"
//! section. Its [`LiquidLeakingInputs`] argument is the set of tables the
//! SQL's "Create Remote Tables" / "Extract Data" sections produce, as plain
//! row vectors; a future Task 50 (`DataFrameStore`) wiring populates it from
//! the per-run filtered execution database.
//!
//! The "Extract Data" `WHERE` clauses are the data-plane contract, not the
//! algorithm, and are left to that wiring: the section filters
//! `EmissionRateByAge` and `OpModeDistribution` to operating modes 150, 151
//! and 300, every `polProcessID`-bearing table to the calculator's single
//! `polProcessID` 113 (THC × Evap Fuel Leaks), `IMCoverage` to the run county,
//! year and `useIMyn = 'Y'`, and the location/time tables to the iteration's
//! county, link, zone and month. [`calculate`](LiquidLeakingCalculator::calculate)
//! therefore treats its inputs as already so filtered; the iteration's year
//! and location flow in through [`LiquidLeakingContext`]. The SQL re-applies
//! the county/year `IMCoverage` filter inside LL-1 as a join condition — that
//! re-check is trivially satisfied by the pre-filtered extract and is not
//! modelled.
//!
//! # Fidelity notes
//!
//! `LiquidLeakingCalculator.sql` stores `WeightedMeanBaseRate.weightedMeanBaseRate`
//! and `.weightedMeanBaseRateIM`, `IMCoverageMergedUngrouped.IMAdjustFract`,
//! and the `emissionQuantIM` working column as `FLOAT` (32-bit), while MariaDB
//! evaluates the arithmetic in `DOUBLE`. This port sums and multiplies in
//! `f64` end to end, so it does not reproduce the `f32` truncation MOVES
//! applies when it writes those intermediates — a sub-`1e-7` relative drift.
//! Reproducing it bug-for-bug is the calculator integration validation call
//! (`mo-fvuf`), matching the `SO2Calculator` / `DistanceCalculator` precedent.
//! `meanBaseRate`, `meanBaseRateIM`, `sourceBinActivityFraction`,
//! `sourceHours`, `opModeFraction`, `IMFactor` and `complianceFactor` are
//! likewise `FLOAT` columns, but they are model *inputs* — already
//! `f32`-quantised before [`calculate`](LiquidLeakingCalculator::calculate)
//! sees them.
//!
//! The only divisions are by the literal `0.01` (and the equivalent in the
//! I/M blend); there are no integer/integer divisions, so the MariaDB
//! `div_precision_increment` rounding gotcha does not arise. The model-year /
//! age relation `modelYearID = year − ageID` is exact integer arithmetic.
//!
//! The `GREATEST(…, 0.0)` clamp on the I/M blend is reproduced with
//! `f64::max`; it bites only when `IMAdjustFract` exceeds 1 and the I/M rate
//! is below the non-I/M rate, which would otherwise drive the blend negative.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric algorithm
//! is fully ported and unit-tested on
//! [`calculate`](LiquidLeakingCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`]. Once the data
//! plane exists, `execute` materialises a [`LiquidLeakingInputs`] from
//! `ctx.tables()`, calls [`calculate`](LiquidLeakingCalculator::calculate),
//! and writes the rows back to `MOVESWorkerOutput`.

use std::collections::{HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `LiquidLeakingCalculator`
/// entry in the calculator-chain DAG.
const CALCULATOR_NAME: &str = "LiquidLeakingCalculator";

/// Total Gaseous Hydrocarbons — `Pollutant` id 1, the pollutant this
/// calculator produces. `CalculatorInfo.txt` records the single `Registration`
/// directive `Total Gaseous Hydrocarbons (1) × Evap Fuel Leaks (13)`.
const THC_POLLUTANT: PollutantId = PollutantId(1);

/// Evap Fuel Leaks — `Process` id 13, the process this calculator subscribes
/// to and registers for. The Java constructor's pollutant/process key `"113"`
/// is `pollutantID × 100 + processID` = `1 × 100 + 13`.
const EVAP_FUEL_LEAKS_PROCESS: ProcessId = ProcessId(13);

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `LiquidLeakingCalculator.sql`'s
// "Extract Data" section pulls that feed the "Processing" section. Following
// the Phase 3 convention, every `INT`/`SMALLINT` identifier is an `i32` (the
// `BIGINT` `sourceBinID` an `i64`) and every `FLOAT`/`DOUBLE` quantity is an
// `f64`. Only the columns the algorithm reads are modelled.
// ===========================================================================

/// The master-loop iteration's year and location — the `##context…##`
/// substitutions the SQL stamps onto the output and uses to derive ages.
///
/// A master-loop invocation is single-county, single-zone, single-link and
/// single-year; the SQL extracts the location/time tables filtered to this
/// context, so the port carries it explicitly rather than re-deriving it from
/// the (single-row) `County` / `Zone` / `Link` extracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiquidLeakingContext {
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
}

/// One `PollutantProcessMappedModelYear` row — the LL-1 model-year driver,
/// resolving a `polProcessID` and model year to its I/M model-year group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollutantProcessMappedModelYearRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — the individual model year.
    pub model_year_id: i32,
    /// `IMModelYearGroupID` — joins to [`ImFactorRow::im_model_year_group_id`].
    pub im_model_year_group_id: i32,
}

/// One `PollutantProcessAssoc` row — a legal `(pollutant, process)` pairing.
///
/// The SQL extracts the rows with `processID = ##context.iterProcess…##`, so
/// every row here is for the Evap Fuel Leaks process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the emission process.
    pub process_id: i32,
    /// `pollutantID` — the pollutant.
    pub pollutant_id: i32,
}

/// One `IMFactor` row — an inspection-and-maintenance program's emission
/// factor for an `(I/M model-year group, age group)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImFactorRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `inspectFreq` — inspection frequency.
    pub inspect_freq: i32,
    /// `testStandardsID` — test standards.
    pub test_standards_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `IMModelYearGroupID`.
    pub im_model_year_group_id: i32,
    /// `ageGroupID` — joins to [`AgeCategoryRow::age_group_id`].
    pub age_group_id: i32,
    /// `IMFactor` — the I/M emission factor. `FLOAT` in MOVES.
    pub im_factor: f64,
}

/// One `AgeCategory` row — maps an `ageID` to its `ageGroupID`.
///
/// `ageID` is the table's key; LL-1 and LL-8 join `AgeCategory` on
/// `ageGroupID` and pin the age via `modelYearID = year − ageID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgeCategoryRow {
    /// `ageID` — vehicle age in years.
    pub age_id: i32,
    /// `ageGroupID` — the age group the age belongs to.
    pub age_group_id: i32,
}

/// One `IMCoverage` row — the extent of an I/M program.
///
/// The SQL extracts `IMCoverage` filtered to the run county and year with
/// `useIMyn = 'Y'`, so `countyID`, `yearID` and `useIMyn` are constant across
/// the extract and are not modelled — see the [module documentation](self).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImCoverageRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `inspectFreq` — joins to [`ImFactorRow::inspect_freq`].
    pub inspect_freq: i32,
    /// `testStandardsID` — joins to [`ImFactorRow::test_standards_id`].
    pub test_standards_id: i32,
    /// `begModelYearID` — first model year the program covers.
    pub beg_model_year_id: i32,
    /// `endModelYearID` — last model year the program covers.
    pub end_model_year_id: i32,
    /// `complianceFactor` — program compliance, as a percentage. `FLOAT` in
    /// MOVES; LL-1 scales it by `0.01`.
    pub compliance_factor: f64,
}

/// One `EmissionRateByAge` row — the leak base rate for a
/// `(source bin, operating mode, age group)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceBinID` — `BIGINT` in MOVES.
    pub source_bin_id: i64,
    /// `opModeID` — the operating mode (150, 151 or 300 for fuel leaks).
    pub op_mode_id: i32,
    /// `ageGroupID` — joins to [`AgeCategoryRow::age_group_id`].
    pub age_group_id: i32,
    /// `meanBaseRate` — the non-I/M base rate. `FLOAT` in MOVES.
    pub mean_base_rate: f64,
    /// `meanBaseRateIM` — the I/M base rate. `FLOAT` in MOVES.
    pub mean_base_rate_im: f64,
}

/// One `SourceBin` row — a source bin's regulatory class, fuel type and
/// model-year group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBinRow {
    /// `sourceBinID` — `BIGINT` in MOVES.
    pub source_bin_id: i64,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `regClassID` — regulatory class, the `WithRegClassID` output dimension.
    pub reg_class_id: i32,
    /// `modelYearGroupID` — joins to
    /// [`PollutantProcessModelYearRow::model_year_group_id`].
    pub model_year_group_id: i32,
}

/// One `FuelType` row — carries the `subjectToEvapCalculations` flag LL-8
/// joins on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelTypeRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `subjectToEvapCalculations = 'Y'` — only fuel types flagged here
    /// contribute to the evaporative leak calculation.
    pub subject_to_evap_calculations: bool,
}

/// One `SourceBinDistribution` row — a source bin's activity share of a
/// `(sourceType, modelYear)` group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID` — joins to
    /// [`SourceTypeModelYearRow::source_type_model_year_id`].
    pub source_type_model_year_id: i32,
    /// `polProcessID` — joined to [`EmissionRateByAgeRow::pol_process_id`].
    pub pol_process_id: i32,
    /// `sourceBinID` — `BIGINT` in MOVES.
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — the bin's activity share. `FLOAT` in
    /// MOVES.
    pub source_bin_activity_fraction: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID` into its
/// source type and model year.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `PollutantProcessModelYear` row — the LL-8 existence filter binding a
/// `(polProcess, modelYear)` to a model-year group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `modelYearGroupID` — joins to [`SourceBinRow::model_year_group_id`].
    pub model_year_group_id: i32,
}

/// One `SourceHours` row — the hours of vehicle activity in a dimension cell.
///
/// The SQL extracts `SourceHours` filtered to the iteration month, year and
/// link; `calculate` joins it on its six-column natural key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID` — vehicle age; LL-9 binds it to `year − modelYearID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `sourceHours` — the activity hours. `FLOAT` in MOVES.
    pub source_hours: f64,
}

/// One `OpModeDistribution` row — the share of a cell's activity spent in an
/// operating mode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `opModeID`.
    pub op_mode_id: i32,
    /// `opModeFraction` — the operating-mode activity share. `FLOAT` in MOVES.
    pub op_mode_fraction: f64,
}

/// One `HourDay` row — resolves an `hourDayID` into its day and hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourDayRow {
    /// `hourDayID` — the combined hour/day key.
    pub hour_day_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
}

/// One `Link` row — LL-9 reads only the link's road type.
///
/// The SQL extracts the single `Link` row for the iteration link; `countyID`
/// and `zoneID` are not read by the "Processing" section (the output stamps
/// them from [`LiquidLeakingContext`]) and are not modelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
}

/// Inputs to [`LiquidLeakingCalculator::calculate`] — the tables the SQL's
/// "Extract Data" section produces, as plain row vectors, plus the iteration
/// [`LiquidLeakingContext`].
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct LiquidLeakingInputs {
    /// The iteration year and location.
    pub context: LiquidLeakingContext,
    /// `PollutantProcessMappedModelYear` rows — the LL-1 model-year driver.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `PollutantProcessAssoc` rows for the Evap Fuel Leaks process.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `IMFactor` rows.
    pub im_factor: Vec<ImFactorRow>,
    /// `AgeCategory` rows — the `ageID → ageGroupID` mapping.
    pub age_category: Vec<AgeCategoryRow>,
    /// `IMCoverage` rows (single county/year, `useIMyn = 'Y'`).
    pub im_coverage: Vec<ImCoverageRow>,
    /// `EmissionRateByAge` rows — the leak base rates.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `FuelType` rows — LL-8 keeps only `subjectToEvapCalculations = 'Y'`.
    pub fuel_type: Vec<FuelTypeRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `PollutantProcessModelYear` rows — the LL-8 existence filter.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `RunSpecMonth` — the run's months; LL-8 cross-joins it.
    pub run_spec_month: Vec<i32>,
    /// `RunSpecHourDay` — the run's hour/day combinations; LL-8 cross-joins it.
    pub run_spec_hour_day: Vec<i32>,
    /// `RunSpecSourceType` — the run's source types; LL-8's existence filter.
    pub run_spec_source_type: Vec<i32>,
    /// `SourceHours` rows.
    pub source_hours: Vec<SourceHoursRow>,
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `Link` rows (the single iteration link).
    pub link: Vec<LinkRow>,
}

/// One liquid-leak emission record produced by the calculation — the
/// algorithm-bearing subset of the `MOVESWorkerOutput` row the SQL inserts.
///
/// `SCC` is a pure pass-through column the SQL writes as `NULL`; it is not
/// modelled. `emissionQuantIM` is a transient working column the SQL drops
/// after applying I/M, so the final row carries only `emissionQuant`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LiquidLeakingEmissionRow {
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
    /// `pollutantID` — always 1 (Total Gaseous Hydrocarbons).
    pub pollutant_id: i32,
    /// `processID` — always 13 (Evap Fuel Leaks).
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
    /// `emissionQuant` — the liquid-leak emission quantity, after I/M.
    pub emission_quant: f64,
}

impl LiquidLeakingEmissionRow {
    /// The integer dimension tuple — every column except `emissionQuant`.
    /// Used to sort the output deterministically: MOVES leaves
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

/// Key into `IMCoverageMergedUngrouped` (LL-1) — the five columns the I/M
/// `UPDATE` matches a `MOVESWorkerOutput` row on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ImAdjustKey {
    process_id: i32,
    pollutant_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    source_type_id: i32,
}

/// Key into `WeightedMeanBaseRate` (LL-8) — the eight `GROUP BY` columns.
///
/// Derives `Ord` so the LL-9 loop can iterate the working table in a stable
/// order, making the output deterministic regardless of hash-map layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct WmbrKey {
    pol_process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    fuel_type_id: i32,
    month_id: i32,
    hour_day_id: i32,
    model_year_id: i32,
    op_mode_id: i32,
}

/// A `WeightedMeanBaseRate` cell — the two activity-weighted base rates.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct WmbrCell {
    /// `Σ sourceBinActivityFraction × meanBaseRate`.
    weighted_mean_base_rate: f64,
    /// `Σ sourceBinActivityFraction × meanBaseRateIM`.
    weighted_mean_base_rate_im: f64,
}

/// Build `IMCoverageMergedUngrouped` — SQL "Processing" step LL-1.
///
/// Disaggregates each `IMCoverage` program record across the individual model
/// years its `[begModelYearID, endModelYearID]` range covers and sums
/// `IMFactor × complianceFactor × 0.01` into an I/M adjustment fraction keyed
/// by `(processID, pollutantID, modelYearID, fuelTypeID, sourceTypeID)`. The
/// `GROUP BY` on those five columns means each key carries a single fraction.
fn im_coverage_merged(inputs: &LiquidLeakingInputs) -> HashMap<ImAdjustKey, f64> {
    // PollutantProcessAssoc by polProcessID (the table's key).
    let ppa_by_pol_process: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|ppa| (ppa.pol_process_id, ppa))
        .collect();
    // IMFactor grouped by (polProcessID, IMModelYearGroupID).
    let mut imf_by_key: HashMap<(i32, i32), Vec<&ImFactorRow>> = HashMap::new();
    for imf in &inputs.im_factor {
        imf_by_key
            .entry((imf.pol_process_id, imf.im_model_year_group_id))
            .or_default()
            .push(imf);
    }
    // IMCoverage grouped by the five exact-match join columns.
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
    // AgeCategory as an (ageID, ageGroupID) membership set: the join is
    // `AgeCategory.ageGroupID = IMFactor.ageGroupID`, gated by the WHERE
    // clause to the single age that resolves the model year.
    let age_categories: HashSet<(i32, i32)> = inputs
        .age_category
        .iter()
        .map(|ac| (ac.age_id, ac.age_group_id))
        .collect();

    let mut im_adjust: HashMap<ImAdjustKey, f64> = HashMap::new();
    for ppmy in &inputs.pollutant_process_mapped_model_year {
        // INNER JOIN PollutantProcessAssoc ON polProcessID.
        let Some(ppa) = ppa_by_pol_process.get(&ppmy.pol_process_id) else {
            continue;
        };
        // INNER JOIN IMFactor ON (polProcessID, IMModelYearGroupID).
        let Some(imfs) = imf_by_key.get(&(ppmy.pol_process_id, ppmy.im_model_year_group_id)) else {
            continue;
        };
        // The WHERE clause `modelYearID = year − ageID` pins the age.
        let age_id = inputs.context.year - ppmy.model_year_id;
        for imf in imfs {
            // INNER JOIN AgeCategory ON ageGroupID, satisfied only when the
            // pinned age belongs to the IMFactor row's age group.
            if !age_categories.contains(&(age_id, imf.age_group_id)) {
                continue;
            }
            // INNER JOIN IMCoverage ON the five exact-match columns.
            let Some(imcs) = imc_by_key.get(&(
                ppmy.pol_process_id,
                imf.inspect_freq,
                imf.test_standards_id,
                imf.source_type_id,
                imf.fuel_type_id,
            )) else {
                continue;
            };
            for imc in imcs {
                // … plus the model-year range. The IMCoverage county/year
                // filter is pre-applied by the extract (see the module docs).
                if imc.beg_model_year_id > ppmy.model_year_id
                    || imc.end_model_year_id < ppmy.model_year_id
                {
                    continue;
                }
                let key = ImAdjustKey {
                    process_id: ppa.process_id,
                    pollutant_id: ppa.pollutant_id,
                    model_year_id: ppmy.model_year_id,
                    fuel_type_id: imf.fuel_type_id,
                    source_type_id: imc.source_type_id,
                };
                *im_adjust.entry(key).or_default() += imf.im_factor * imc.compliance_factor * 0.01;
            }
        }
    }
    im_adjust
}

/// Build `WeightedMeanBaseRate` — SQL "Processing" step LL-8, `WithRegClassID`
/// variant.
///
/// Sums `sourceBinActivityFraction × meanBaseRate` (and the `…IM` variant)
/// over the source bins joined to the leak base rates, then cross-joins the
/// run's months and hour/day combinations, keying the result by the eight
/// `GROUP BY` columns.
fn weighted_mean_base_rate(inputs: &LiquidLeakingInputs) -> HashMap<WmbrKey, WmbrCell> {
    let source_bin_by_id: HashMap<i64, &SourceBinRow> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb))
        .collect();
    // FuelType ⋈ subjectToEvapCalculations = 'Y' — an existence filter.
    let evap_fuel_types: HashSet<i32> = inputs
        .fuel_type
        .iter()
        .filter(|ft| ft.subject_to_evap_calculations)
        .map(|ft| ft.fuel_type_id)
        .collect();
    let mut sbd_by_bin: HashMap<i64, Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in &inputs.source_bin_distribution {
        sbd_by_bin.entry(sbd.source_bin_id).or_default().push(sbd);
    }
    let stmy_by_id: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();
    // PollutantProcessModelYear as a (polProcessID, modelYearID,
    // modelYearGroupID) membership set — LL-8's existence filter.
    let ppmy_set: HashSet<(i32, i32, i32)> = inputs
        .pollutant_process_model_year
        .iter()
        .map(|ppmy| {
            (
                ppmy.pol_process_id,
                ppmy.model_year_id,
                ppmy.model_year_group_id,
            )
        })
        .collect();
    let run_spec_source_type: HashSet<i32> = inputs.run_spec_source_type.iter().copied().collect();
    let age_categories: HashSet<(i32, i32)> = inputs
        .age_category
        .iter()
        .map(|ac| (ac.age_id, ac.age_group_id))
        .collect();

    let mut wmbr: HashMap<WmbrKey, WmbrCell> = HashMap::new();
    for er in &inputs.emission_rate_by_age {
        // INNER JOIN SourceBin ON sourceBinID.
        let Some(sb) = source_bin_by_id.get(&er.source_bin_id) else {
            continue;
        };
        // INNER JOIN FuelType ON fuelTypeID AND subjectToEvapCalculations='Y'.
        if !evap_fuel_types.contains(&sb.fuel_type_id) {
            continue;
        }
        // INNER JOIN SourceBinDistribution ON sourceBinID AND polProcessID.
        let Some(sbds) = sbd_by_bin.get(&er.source_bin_id) else {
            continue;
        };
        for sbd in sbds {
            if sbd.pol_process_id != er.pol_process_id {
                continue;
            }
            // INNER JOIN SourceTypeModelYear ON sourceTypeModelYearID.
            let Some(stmy) = stmy_by_id.get(&sbd.source_type_model_year_id) else {
                continue;
            };
            // … AND stmy.modelYearID = year − AgeCategory.ageID, with
            // AgeCategory joined on EmissionRateByAge.ageGroupID.
            let age_id = inputs.context.year - stmy.model_year_id;
            if !age_categories.contains(&(age_id, er.age_group_id)) {
                continue;
            }
            // INNER JOIN PollutantProcessModelYear ON
            // (polProcessID, modelYearID, modelYearGroupID).
            if !ppmy_set.contains(&(
                sbd.pol_process_id,
                stmy.model_year_id,
                sb.model_year_group_id,
            )) {
                continue;
            }
            // INNER JOIN RunSpecSourceType ON sourceTypeID.
            if !run_spec_source_type.contains(&stmy.source_type_id) {
                continue;
            }
            // CROSS JOIN RunSpecMonth, RunSpecHourDay — the month and hour/day
            // come from the run spec, not from any joined data row.
            for &month_id in &inputs.run_spec_month {
                for &hour_day_id in &inputs.run_spec_hour_day {
                    let key = WmbrKey {
                        pol_process_id: er.pol_process_id,
                        source_type_id: stmy.source_type_id,
                        reg_class_id: sb.reg_class_id,
                        fuel_type_id: sb.fuel_type_id,
                        month_id,
                        hour_day_id,
                        model_year_id: stmy.model_year_id,
                        op_mode_id: er.op_mode_id,
                    };
                    let cell = wmbr.entry(key).or_default();
                    cell.weighted_mean_base_rate +=
                        sbd.source_bin_activity_fraction * er.mean_base_rate;
                    cell.weighted_mean_base_rate_im +=
                        sbd.source_bin_activity_fraction * er.mean_base_rate_im;
                }
            }
        }
    }
    wmbr
}

/// The MOVES liquid-leaking calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input flows
/// through the [`LiquidLeakingInputs`] argument to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct LiquidLeakingCalculator {
    /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl LiquidLeakingCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator with its master-loop subscription.
    ///
    /// `CalculatorInfo.txt` records one `Subscribe` directive for
    /// `LiquidLeakingCalculator`: the Evap Fuel Leaks process (13) at `MONTH`
    /// granularity with `EMISSION_CALCULATOR` priority — the Java constructor
    /// passes `MasterLoopGranularity.MONTH` and offset `0` from the standard
    /// `EMISSION_CALCULATOR` priority.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                EVAP_FUEL_LEAKS_PROCESS,
                Granularity::Month,
                priority,
            )],
        }
    }

    /// Compute the liquid-leak emission rows — the port of the
    /// `LiquidLeakingCalculator.sql` "Processing" section.
    ///
    /// Returns no rows when no dimension cell survives every `INNER JOIN`: an
    /// `EmissionRateByAge` row contributes only if its source bin resolves a
    /// `subjectToEvapCalculations` fuel type, a source-bin distribution, a
    /// source-type/model-year, an age category and a pollutant-process model
    /// year, and the resulting `WeightedMeanBaseRate` cell then resolves a
    /// `SourceHours` row, an `OpModeDistribution` row, a pollutant/process, a
    /// link and an hour/day. The result is sorted by its integer dimension
    /// columns for deterministic output; MOVES leaves `MOVESWorkerOutput`
    /// physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &LiquidLeakingInputs) -> Vec<LiquidLeakingEmissionRow> {
        // --- LL-1: IMCoverageMergedUngrouped --------------------------------
        let im_adjust = im_coverage_merged(inputs);

        // --- LL-8: WeightedMeanBaseRate -------------------------------------
        let wmbr = weighted_mean_base_rate(inputs);

        // --- LL-9 + Apply I/M -----------------------------------------------
        // SourceHours by its six-column natural key.
        let source_hours_by_key: HashMap<(i32, i32, i32, i32, i32, i32), &SourceHoursRow> = inputs
            .source_hours
            .iter()
            .map(|sh| {
                (
                    (
                        sh.hour_day_id,
                        sh.month_id,
                        sh.year_id,
                        sh.age_id,
                        sh.link_id,
                        sh.source_type_id,
                    ),
                    sh,
                )
            })
            .collect();
        // OpModeDistribution by its five-column natural key.
        let omd_by_key: HashMap<(i32, i32, i32, i32, i32), &OpModeDistributionRow> = inputs
            .op_mode_distribution
            .iter()
            .map(|omd| {
                (
                    (
                        omd.source_type_id,
                        omd.hour_day_id,
                        omd.link_id,
                        omd.pol_process_id,
                        omd.op_mode_id,
                    ),
                    omd,
                )
            })
            .collect();
        let ppa_by_pol_process: HashMap<i32, &PollutantProcessAssocRow> = inputs
            .pollutant_process_assoc
            .iter()
            .map(|ppa| (ppa.pol_process_id, ppa))
            .collect();
        let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
            .hour_day
            .iter()
            .map(|hd| (hd.hour_day_id, hd))
            .collect();
        let link_by_id: HashMap<i32, &LinkRow> =
            inputs.link.iter().map(|l| (l.link_id, l)).collect();

        let ctx = &inputs.context;

        // Iterate WeightedMeanBaseRate in a stable key order so the output is
        // deterministic regardless of hash-map layout.
        let mut wmbr_entries: Vec<(&WmbrKey, &WmbrCell)> = wmbr.iter().collect();
        wmbr_entries.sort_unstable_by_key(|&(key, _)| *key);

        let mut out: Vec<LiquidLeakingEmissionRow> = Vec::new();
        for (key, cell) in wmbr_entries {
            // INNER JOIN SourceHours — `ageID = year − modelYearID`, `linkID`
            // the iteration link.
            let sh_key = (
                key.hour_day_id,
                key.month_id,
                ctx.year,
                ctx.year - key.model_year_id,
                ctx.link_id,
                key.source_type_id,
            );
            let Some(sh) = source_hours_by_key.get(&sh_key) else {
                continue;
            };
            // INNER JOIN OpModeDistribution — `hourDayID`/`opModeID`/
            // `polProcessID` from the working-table row, `linkID` the
            // iteration link.
            let omd_key = (
                key.source_type_id,
                key.hour_day_id,
                ctx.link_id,
                key.pol_process_id,
                key.op_mode_id,
            );
            let Some(omd) = omd_by_key.get(&omd_key) else {
                continue;
            };
            // INNER JOIN PollutantProcessAssoc ON polProcessID.
            let Some(ppa) = ppa_by_pol_process.get(&key.pol_process_id) else {
                continue;
            };
            // INNER JOIN Link ON the iteration link.
            let Some(link) = link_by_id.get(&ctx.link_id) else {
                continue;
            };
            // INNER JOIN HourDay ON hourDayID.
            let Some(hd) = hour_day_by_id.get(&key.hour_day_id) else {
                continue;
            };

            // LL-9: emissionQuant = weightedMeanBaseRate × sourceHours ×
            // opModeFraction; emissionQuantIM the I/M-rate counterpart.
            let mut emission_quant =
                cell.weighted_mean_base_rate * sh.source_hours * omd.op_mode_fraction;
            let emission_quant_im =
                cell.weighted_mean_base_rate_im * sh.source_hours * omd.op_mode_fraction;

            // Apply I/M: emissionQuant = GREATEST(emissionQuantIM × IMAdjustFract
            // + emissionQuant × (1 − IMAdjustFract), 0). A row with no matching
            // IMCoverageMergedUngrouped cell keeps its value — the SQL UPDATE
            // leaves unmatched rows untouched.
            let im_key = ImAdjustKey {
                process_id: ppa.process_id,
                pollutant_id: ppa.pollutant_id,
                model_year_id: key.model_year_id,
                fuel_type_id: key.fuel_type_id,
                source_type_id: key.source_type_id,
            };
            if let Some(&im_adjust_fract) = im_adjust.get(&im_key) {
                emission_quant = (emission_quant_im * im_adjust_fract
                    + emission_quant * (1.0 - im_adjust_fract))
                    .max(0.0);
            }

            out.push(LiquidLeakingEmissionRow {
                year_id: ctx.year,
                month_id: key.month_id,
                day_id: hd.day_id,
                hour_id: hd.hour_id,
                state_id: ctx.state_id,
                county_id: ctx.county_id,
                zone_id: ctx.zone_id,
                link_id: ctx.link_id,
                pollutant_id: ppa.pollutant_id,
                process_id: ppa.process_id,
                source_type_id: key.source_type_id,
                reg_class_id: key.reg_class_id,
                fuel_type_id: key.fuel_type_id,
                model_year_id: key.model_year_id,
                road_type_id: link.road_type_id,
                emission_quant,
            });
        }

        // Stable sort by the dimension columns: two rows can share a dimension
        // key (they differ only in the operating mode, which the output row
        // does not carry), and a stable sort keeps them in WeightedMeanBaseRate
        // key order.
        out.sort_by_key(LiquidLeakingEmissionRow::dimension_key);
        out
    }
}

impl Default for LiquidLeakingCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// The one `(pollutant, process)` pair the calculator registers — Total
/// Gaseous Hydrocarbons (1) for Evap Fuel Leaks (13), the single `Registration`
/// directive recorded for `LiquidLeakingCalculator` in `CalculatorInfo.txt`.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[PollutantProcessAssociation {
    pollutant_id: THC_POLLUTANT,
    process_id: EVAP_FUEL_LEAKS_PROCESS,
}];

// ===========================================================================
// TableRow implementations for all LL input/output types plus helper row
// structs for ID-only tables (RunSpec*).
// ===========================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction { table: table.into(), row, column: column.into(), message: msg }
}

struct RunSpecMonthIdRow { month_id: i32 }
struct RunSpecHourDayIdRow { hour_day_id: i32 }
struct RunSpecSourceTypeIdRow { source_type_id: i32 }

impl TableRow for RunSpecMonthIdRow {
    fn table_name() -> &'static str { "RunSpecMonth" }
    fn polars_schema() -> Schema { Schema::from_iter([("monthID".into(), DataType::Int32)]) }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into()])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecMonth";
        let month_id = df.column("monthID").map_err(|e| row_err(t, 0, "monthID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        (0..df.height()).map(|i| Ok(RunSpecMonthIdRow { month_id: month_id.get(i).ok_or_else(|| row_err(t, i, "monthID", "null value".into()))? })).collect()
    }
}

impl TableRow for RunSpecHourDayIdRow {
    fn table_name() -> &'static str { "RunSpecHourDay" }
    fn polars_schema() -> Schema { Schema::from_iter([("hourDayID".into(), DataType::Int32)]) }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into()])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecHourDay";
        let hour_day_id = df.column("hourDayID").map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height()).map(|i| Ok(RunSpecHourDayIdRow { hour_day_id: hour_day_id.get(i).ok_or_else(|| row_err(t, i, "hourDayID", "null value".into()))? })).collect()
    }
}

impl TableRow for RunSpecSourceTypeIdRow {
    fn table_name() -> &'static str { "RunSpecSourceType" }
    fn polars_schema() -> Schema { Schema::from_iter([("sourceTypeID".into(), DataType::Int32)]) }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into()])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecSourceType";
        let source_type_id = df.column("sourceTypeID").map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height()).map(|i| Ok(RunSpecSourceTypeIdRow { source_type_id: source_type_id.get(i).ok_or_else(|| row_err(t, i, "sourceTypeID", "null value".into()))? })).collect()
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
        let pol_process_id = get_i32("polProcessID")?;
        let process_id = get_i32("processID")?;
        let pollutant_id = get_i32("pollutantID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessAssocRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
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

impl TableRow for ImCoverageRow {
    fn table_name() -> &'static str { "IMCoverage" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("begModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("complianceFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("inspectFreq".into(), rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>()).into(),
            Series::new("testStandardsID".into(), rows.iter().map(|r| r.test_standards_id).collect::<Vec<i32>>()).into(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let beg_model_year_id = get_i32("begModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let compliance_factor = get_f64("complianceFactor")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(ImCoverageRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                test_standards_id: test_standards_id.get(i).ok_or_else(|| null("testStandardsID"))?,
                beg_model_year_id: beg_model_year_id.get(i).ok_or_else(|| null("begModelYearID"))?,
                end_model_year_id: end_model_year_id.get(i).ok_or_else(|| null("endModelYearID"))?,
                compliance_factor: compliance_factor.get(i).ok_or_else(|| null("complianceFactor"))?,
            })
        }).collect()
    }
}

impl TableRow for EmissionRateByAgeRow {
    fn table_name() -> &'static str { "EmissionRateByAge" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("opModeID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceBinID".into(), rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>()).into(),
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
        let pol_process_id = get_i32("polProcessID")?;
        let source_bin_id = get_i64("sourceBinID")?;
        let op_mode_id = get_i32("opModeID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let mean_base_rate = get_f64("meanBaseRate")?;
        let mean_base_rate_im = get_f64("meanBaseRateIM")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(EmissionRateByAgeRow {
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
                mean_base_rate_im: mean_base_rate_im.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
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
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_bin_id = get_i64("sourceBinID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceBinRow {
                source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                model_year_group_id: model_year_group_id.get(i).ok_or_else(|| null("modelYearGroupID"))?,
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
        let fuel_type_id = df.column("fuelTypeID").map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?.i32().map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        let subject_to_evap = df.column("subjectToEvapCalculations").map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?.bool().map_err(|e| row_err(t, 0, "subjectToEvapCalculations", e.to_string()))?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelTypeRow {
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                subject_to_evap_calculations: subject_to_evap.get(i).ok_or_else(|| null("subjectToEvapCalculations"))?,
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

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str { "SourceHours" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
            Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceHours".into(), rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceHours";
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
        let link_id = get_i32("linkID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let source_hours = get_f64("sourceHours")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SourceHoursRow {
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                source_hours: source_hours.get(i).ok_or_else(|| null("sourceHours"))?,
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let link_id = get_i32("linkID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(OpModeDistributionRow {
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                op_mode_fraction: op_mode_fraction.get(i).ok_or_else(|| null("opModeFraction"))?,
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

impl TableRow for LinkRow {
    fn table_name() -> &'static str { "Link" }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let link_id = get_i32("linkID")?;
        let road_type_id = get_i32("roadTypeID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(LinkRow {
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
            })
        }).collect()
    }
}

impl TableRow for LiquidLeakingEmissionRow {
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
            ("regClassID".into(), DataType::Int32),
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
            Series::new("regClassID".into(), rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>()).into(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
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
        let reg_class_id = get_i32("regClassID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(LiquidLeakingEmissionRow {
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
                reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
            })
        }).collect()
    }
}

/// Default-DB / execution-DB tables the liquid-leak computation consumes — the
/// data tables the SQL's "Extract Data" section pulls that feed the
/// "Processing" section. The SQL also extracts `County`, `Zone`,
/// `MonthOfAnyYear`, `Year` and `RegClassSourceTypeFraction`; none feeds the
/// "Processing" section, so none is listed.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "EmissionRateByAge",
    "FuelType",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "Link",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "PollutantProcessModelYear",
    "RunSpecHourDay",
    "RunSpecMonth",
    "RunSpecSourceType",
    "SourceBin",
    "SourceBinDistribution",
    "SourceHours",
    "SourceTypeModelYear",
];

impl Calculator for LiquidLeakingCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty). `LiquidLeakingCalculator` is
    // a direct MasterLoop subscriber; the one `Chain` directive that names it
    // has it as the inModule (`HCSpeciationCalculator` chains off it), so the
    // dependency edge runs the other way and this calculator has no upstream.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let liquid_ctx = LiquidLeakingContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
        };
        let inputs = LiquidLeakingInputs {
            context: liquid_ctx,
            pollutant_process_mapped_model_year: tables.iter_typed::<PollutantProcessMappedModelYearRow>("PollutantProcessMappedModelYear")?,
            pollutant_process_assoc: tables.iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
            im_factor: tables.iter_typed::<ImFactorRow>("IMFactor")?,
            age_category: tables.iter_typed::<AgeCategoryRow>("AgeCategory")?,
            im_coverage: tables.iter_typed::<ImCoverageRow>("IMCoverage")?,
            emission_rate_by_age: tables.iter_typed::<EmissionRateByAgeRow>("EmissionRateByAge")?,
            source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
            fuel_type: tables.iter_typed::<FuelTypeRow>("FuelType")?,
            source_bin_distribution: tables.iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
            source_type_model_year: tables.iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
            pollutant_process_model_year: tables.iter_typed::<PollutantProcessModelYearRow>("PollutantProcessModelYear")?,
            run_spec_month: tables.iter_typed::<RunSpecMonthIdRow>("RunSpecMonth")?.into_iter().map(|r| r.month_id).collect(),
            run_spec_hour_day: tables.iter_typed::<RunSpecHourDayIdRow>("RunSpecHourDay")?.into_iter().map(|r| r.hour_day_id).collect(),
            run_spec_source_type: tables.iter_typed::<RunSpecSourceTypeIdRow>("RunSpecSourceType")?.into_iter().map(|r| r.source_type_id).collect(),
            source_hours: tables.iter_typed::<SourceHoursRow>("SourceHours")?,
            op_mode_distribution: tables.iter_typed::<OpModeDistributionRow>("OpModeDistribution")?,
            hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
            link: tables.iter_typed::<LinkRow>("Link")?,
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(LiquidLeakingCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a one-bin / one-rate input whose single output row has
    /// `emission_quant == 2.4`:
    ///
    /// * LL-1: `IMAdjustFract = 0.5 × 80.0 × 0.01 = 0.4`.
    /// * LL-8: `weightedMeanBaseRate = 0.5 × 2.0 = 1.0`,
    ///   `weightedMeanBaseRateIM = 0.5 × 1.0 = 0.5`.
    /// * LL-9: `emissionQuant = 1.0 × 10.0 × 0.3 = 3.0`,
    ///   `emissionQuantIM = 0.5 × 10.0 × 0.3 = 1.5`.
    /// * I/M: `max(1.5 × 0.4 + 3.0 × 0.6, 0) = 0.6 + 1.8 = 2.4`.
    ///
    /// Values are chosen for an exact result, not physical realism.
    fn minimal_inputs() -> LiquidLeakingInputs {
        LiquidLeakingInputs {
            context: LiquidLeakingContext {
                year: 2020,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
            },
            pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
                pol_process_id: 113, // THC (1) × Evap Fuel Leaks (13)
                model_year_id: 2018,
                im_model_year_group_id: 7,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 113,
                process_id: 13,
                pollutant_id: 1,
            }],
            im_factor: vec![ImFactorRow {
                pol_process_id: 113,
                inspect_freq: 1,
                test_standards_id: 2,
                source_type_id: 21,
                fuel_type_id: 1,
                im_model_year_group_id: 7,
                age_group_id: 3,
                im_factor: 0.5,
            }],
            age_category: vec![AgeCategoryRow {
                age_id: 2, // 2020 − 2018
                age_group_id: 3,
            }],
            im_coverage: vec![ImCoverageRow {
                pol_process_id: 113,
                source_type_id: 21,
                fuel_type_id: 1,
                inspect_freq: 1,
                test_standards_id: 2,
                beg_model_year_id: 2000,
                end_model_year_id: 2030,
                compliance_factor: 80.0,
            }],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                pol_process_id: 113,
                source_bin_id: 500,
                op_mode_id: 150,
                age_group_id: 3,
                mean_base_rate: 2.0,
                mean_base_rate_im: 1.0,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 500,
                fuel_type_id: 1,
                reg_class_id: 30,
                model_year_group_id: 6,
            }],
            fuel_type: vec![FuelTypeRow {
                fuel_type_id: 1,
                subject_to_evap_calculations: true,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 9001,
                pol_process_id: 113,
                source_bin_id: 500,
                source_bin_activity_fraction: 0.5,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 9001,
                model_year_id: 2018,
                source_type_id: 21,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 113,
                model_year_id: 2018,
                model_year_group_id: 6,
            }],
            run_spec_month: vec![7],
            run_spec_hour_day: vec![85],
            run_spec_source_type: vec![21],
            source_hours: vec![SourceHoursRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2,
                link_id: 5001,
                source_type_id: 21,
                source_hours: 10.0,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 85,
                link_id: 5001,
                pol_process_id: 113,
                op_mode_id: 150,
                op_mode_fraction: 0.3,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                road_type_id: 5,
            }],
        }
    }

    /// Assert `actual` matches `expected` within `f64` slack — the
    /// FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = LiquidLeakingCalculator::new().calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // The dimension cell — year and location from the context, the rest
        // carried through the working tables.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 261_610);
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.pollutant_id, 1); // Total Gaseous Hydrocarbons
        assert_eq!(r.process_id, 13); // Evap Fuel Leaks
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 1);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 5);
        // weightedMeanBaseRate 1.0 → emissionQuant 3.0, blended with the I/M
        // rate at IMAdjustFract 0.4 → 1.5 × 0.4 + 3.0 × 0.6 = 2.4.
        assert_close(r.emission_quant, 2.4);
    }

    #[test]
    fn calculate_leaves_emission_unadjusted_without_im_coverage() {
        // With no IMCoverage there is no IMCoverageMergedUngrouped cell, so the
        // I/M UPDATE leaves the row untouched: emissionQuant stays at the LL-9
        // value 1.0 × 10.0 × 0.3 = 3.0.
        let mut inputs = minimal_inputs();
        inputs.im_coverage.clear();
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 3.0);
    }

    #[test]
    fn calculate_leaves_emission_unadjusted_when_im_is_for_another_source_type() {
        // An I/M program for source type 99 builds an IMCoverageMergedUngrouped
        // cell keyed to source type 99; the output row is source type 21, so
        // the I/M UPDATE's join misses and the row keeps its LL-9 value.
        let mut inputs = minimal_inputs();
        inputs.im_factor[0].source_type_id = 99;
        inputs.im_coverage[0].source_type_id = 99;
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 3.0);
    }

    #[test]
    fn calculate_clamps_negative_im_blend_to_zero() {
        // IMAdjustFract = 2.5 × 80.0 × 0.01 = 2.0 and a zero I/M rate drive the
        // blend negative: 0.0 × 2.0 + 3.0 × (1 − 2.0) = −3.0; GREATEST(…, 0)
        // clamps it to 0.
        let mut inputs = minimal_inputs();
        inputs.im_factor[0].im_factor = 2.5;
        inputs.emission_rate_by_age[0].mean_base_rate_im = 0.0;
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 0.0);
    }

    #[test]
    fn calculate_sums_im_adjustment_over_overlapping_programs() {
        // Two IMCoverage programs cover model year 2018; LL-1 sums their
        // contributions: IMAdjustFract = 0.4 + 0.4 = 0.8. The programs differ
        // in test-standards id so both join the (matching) IMFactor rows.
        let mut inputs = minimal_inputs();
        inputs.im_factor.push(ImFactorRow {
            test_standards_id: 9,
            ..inputs.im_factor[0]
        });
        inputs.im_coverage.push(ImCoverageRow {
            test_standards_id: 9,
            ..inputs.im_coverage[0]
        });
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        // max(1.5 × 0.8 + 3.0 × 0.2, 0) = 1.2 + 0.6 = 1.8.
        assert_close(rows[0].emission_quant, 1.8);
    }

    #[test]
    fn calculate_weights_base_rate_by_source_bin_activity_fraction() {
        // A second source bin of the same dimension cell adds to the same
        // WeightedMeanBaseRate key: weightedMeanBaseRate = 0.5 × 2.0 +
        // 0.25 × 4.0 = 2.0. Drop I/M to isolate the LL-8 weighting.
        let mut inputs = minimal_inputs();
        inputs.im_coverage.clear();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 600,
            fuel_type_id: 1,
            reg_class_id: 30,
            model_year_group_id: 6,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 9001,
                pol_process_id: 113,
                source_bin_id: 600,
                source_bin_activity_fraction: 0.25,
            });
        inputs.emission_rate_by_age.push(EmissionRateByAgeRow {
            pol_process_id: 113,
            source_bin_id: 600,
            op_mode_id: 150,
            age_group_id: 3,
            mean_base_rate: 4.0,
            mean_base_rate_im: 0.0,
        });
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        // emissionQuant = 2.0 × 10.0 × 0.3 = 6.0.
        assert_close(rows[0].emission_quant, 6.0);
    }

    #[test]
    fn calculate_cross_joins_run_spec_hour_days() {
        // Two RunSpecHourDay rows cross-join into two WeightedMeanBaseRate
        // cells; with a SourceHours / OpModeDistribution / HourDay row for each
        // hour/day, LL-9 emits one output row per hour/day.
        let mut inputs = minimal_inputs();
        inputs.run_spec_hour_day = vec![85, 109];
        inputs.source_hours.push(SourceHoursRow {
            hour_day_id: 109,
            ..inputs.source_hours[0]
        });
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 109,
            ..inputs.op_mode_distribution[0]
        });
        inputs.hour_day.push(HourDayRow {
            hour_day_id: 109,
            day_id: 2,
            hour_id: 13,
        });
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        // Same emission, distinct hour/day dimensions.
        for r in &rows {
            assert_close(r.emission_quant, 2.4);
        }
        assert!(rows.iter().any(|r| r.hour_id == 8 && r.day_id == 5));
        assert!(rows.iter().any(|r| r.hour_id == 13 && r.day_id == 2));
    }

    #[test]
    fn calculate_drops_fuel_type_not_subject_to_evap() {
        // A fuel type with subjectToEvapCalculations = 'N' fails the LL-8
        // FuelType join — no WeightedMeanBaseRate cell, no output.
        let mut inputs = minimal_inputs();
        inputs.fuel_type[0].subject_to_evap_calculations = false;
        assert!(LiquidLeakingCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_rows_without_an_ll8_join() {
        // Each LL-8 inner join, removed in turn, drops the only dimension cell.
        let base = minimal_inputs();

        let mut no_source_bin = base.clone();
        no_source_bin.source_bin.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_source_bin)
            .is_empty());

        let mut no_sbd = base.clone();
        no_sbd.source_bin_distribution.clear();
        assert!(LiquidLeakingCalculator::new().calculate(&no_sbd).is_empty());

        let mut no_stmy = base.clone();
        no_stmy.source_type_model_year.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_stmy)
            .is_empty());

        let mut no_ppmy = base.clone();
        no_ppmy.pollutant_process_model_year.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_ppmy)
            .is_empty());

        let mut no_run_source_type = base.clone();
        no_run_source_type.run_spec_source_type.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_run_source_type)
            .is_empty());

        // No AgeCategory row resolves the model year to an age.
        let mut no_age = base;
        no_age.age_category.clear();
        assert!(LiquidLeakingCalculator::new().calculate(&no_age).is_empty());
    }

    #[test]
    fn calculate_drops_rows_without_an_ll9_join() {
        // Each LL-9 inner join, removed in turn, drops the output row.
        let base = minimal_inputs();

        let mut no_source_hours = base.clone();
        no_source_hours.source_hours.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_source_hours)
            .is_empty());

        let mut no_op_mode = base.clone();
        no_op_mode.op_mode_distribution.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_op_mode)
            .is_empty());

        let mut no_ppa = base.clone();
        no_ppa.pollutant_process_assoc.clear();
        assert!(LiquidLeakingCalculator::new().calculate(&no_ppa).is_empty());

        let mut no_link = base.clone();
        no_link.link.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_link)
            .is_empty());

        let mut no_hour_day = base;
        no_hour_day.hour_day.clear();
        assert!(LiquidLeakingCalculator::new()
            .calculate(&no_hour_day)
            .is_empty());
    }

    #[test]
    fn calculate_drops_source_hours_for_the_wrong_age() {
        // SourceHours is joined on `ageID = year − modelYearID`; an age that
        // does not match the model year (2020 − 2018 = 2) misses the join.
        let mut inputs = minimal_inputs();
        inputs.source_hours[0].age_id = 5;
        assert!(LiquidLeakingCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_op_mode_distribution_for_a_different_op_mode() {
        // OpModeDistribution is joined on the WeightedMeanBaseRate cell's
        // operating mode; a row for another op mode misses the join.
        let mut inputs = minimal_inputs();
        inputs.op_mode_distribution[0].op_mode_id = 300;
        assert!(LiquidLeakingCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two hour/day rows produce two output rows; the result comes back
        // dimension-key sorted regardless of input order. hourDay 109 (day 2,
        // hour 13) sorts before hourDay 85 (day 5, hour 8).
        let mut inputs = minimal_inputs();
        inputs.run_spec_hour_day = vec![85, 109];
        inputs.source_hours.push(SourceHoursRow {
            hour_day_id: 109,
            ..inputs.source_hours[0]
        });
        inputs.op_mode_distribution.push(OpModeDistributionRow {
            hour_day_id: 109,
            ..inputs.op_mode_distribution[0]
        });
        inputs.hour_day.push(HourDayRow {
            hour_day_id: 109,
            day_id: 2,
            hour_id: 13,
        });
        let rows = LiquidLeakingCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
        assert_eq!((rows[0].day_id, rows[0].hour_id), (2, 13));
        assert_eq!((rows[1].day_id, rows[1].hour_id), (5, 8));
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(LiquidLeakingCalculator::new()
            .calculate(&LiquidLeakingInputs::default())
            .is_empty());
    }

    #[test]
    fn calculator_name_matches_module() {
        assert_eq!(
            LiquidLeakingCalculator::new().name(),
            "LiquidLeakingCalculator"
        );
        assert_eq!(LiquidLeakingCalculator::NAME, "LiquidLeakingCalculator");
    }

    #[test]
    fn calculator_subscribes_to_evap_fuel_leaks_at_month_granularity() {
        // CalculatorInfo.txt: Subscribe LiquidLeakingCalculator Evap Fuel Leaks
        // 13 MONTH EMISSION_CALCULATOR.
        let calc = LiquidLeakingCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(13));
        assert_eq!(subs[0].granularity, Granularity::Month);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");
    }

    #[test]
    fn calculator_registers_thc_for_evap_fuel_leaks() {
        // CalculatorInfo.txt: Registration Total Gaseous Hydrocarbons 1
        // Evap Fuel Leaks 13 LiquidLeakingCalculator.
        let calc = LiquidLeakingCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 1);
        assert_eq!(regs[0].pollutant_id, PollutantId(1));
        assert_eq!(regs[0].process_id, ProcessId(13));
    }

    #[test]
    fn calculator_has_no_upstream() {
        // A direct subscriber: the only Chain directive naming the calculator
        // has it as the inModule, so it depends on nothing upstream.
        assert!(LiquidLeakingCalculator::new().upstream().is_empty());
    }

    #[test]
    fn calculator_declares_input_tables() {
        let calc = LiquidLeakingCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "AgeCategory",
            "EmissionRateByAge",
            "FuelType",
            "HourDay",
            "IMCoverage",
            "IMFactor",
            "Link",
            "OpModeDistribution",
            "PollutantProcessAssoc",
            "PollutantProcessMappedModelYear",
            "PollutantProcessModelYear",
            "RunSpecHourDay",
            "RunSpecMonth",
            "RunSpecSourceType",
            "SourceBin",
            "SourceBinDistribution",
            "SourceHours",
            "SourceTypeModelYear",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let inputs = minimal_inputs();
        let ctx_vals = inputs.context;
        let mut store = InMemoryStore::new();
        store.insert("PollutantProcessMappedModelYear", PollutantProcessMappedModelYearRow::into_dataframe(inputs.pollutant_process_mapped_model_year).unwrap());
        store.insert("PollutantProcessAssoc", PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc).unwrap());
        store.insert("IMFactor", ImFactorRow::into_dataframe(inputs.im_factor).unwrap());
        store.insert("AgeCategory", AgeCategoryRow::into_dataframe(inputs.age_category).unwrap());
        store.insert("IMCoverage", ImCoverageRow::into_dataframe(inputs.im_coverage).unwrap());
        store.insert("EmissionRateByAge", EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age).unwrap());
        store.insert("SourceBin", SourceBinRow::into_dataframe(inputs.source_bin).unwrap());
        store.insert("FuelType", FuelTypeRow::into_dataframe(inputs.fuel_type).unwrap());
        store.insert("SourceBinDistribution", SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution).unwrap());
        store.insert("SourceTypeModelYear", SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year).unwrap());
        store.insert("PollutantProcessModelYear", PollutantProcessModelYearRow::into_dataframe(inputs.pollutant_process_model_year).unwrap());
        store.insert("RunSpecMonth", RunSpecMonthIdRow::into_dataframe(
            inputs.run_spec_month.iter().map(|&month_id| RunSpecMonthIdRow { month_id }).collect()
        ).unwrap());
        store.insert("RunSpecHourDay", RunSpecHourDayIdRow::into_dataframe(
            inputs.run_spec_hour_day.iter().map(|&hour_day_id| RunSpecHourDayIdRow { hour_day_id }).collect()
        ).unwrap());
        store.insert("RunSpecSourceType", RunSpecSourceTypeIdRow::into_dataframe(
            inputs.run_spec_source_type.iter().map(|&source_type_id| RunSpecSourceTypeIdRow { source_type_id }).collect()
        ).unwrap());
        store.insert("SourceHours", SourceHoursRow::into_dataframe(inputs.source_hours).unwrap());
        store.insert("OpModeDistribution", OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution).unwrap());
        store.insert("HourDay", HourDayRow::into_dataframe(inputs.hour_day).unwrap());
        store.insert("Link", LinkRow::into_dataframe(inputs.link).unwrap());
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(
                ctx_vals.state_id as u32,
                ctx_vals.county_id as u32,
                ctx_vals.zone_id as u32,
                ctx_vals.link_id as u32,
            ),
            time: ExecutionTime::year(ctx_vals.year as u16),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let out = LiquidLeakingCalculator::new().execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("expected non-empty DataFrame");
        assert_eq!(df.height(), 1, "minimal inputs produce exactly one liquid leaking row");
        let quant = df.column("emissionQuant").unwrap().f64().unwrap().get(0).unwrap();
        // weightedMeanBaseRate 1.0 → emissionQuant 3.0, blended with the I/M
        // rate at IMAdjustFract 0.4 → 1.5 × 0.4 + 3.0 × 0.6 = 2.4.
        assert!((quant - 2.4).abs() < 1e-9, "emissionQuant {quant} != 2.4");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "LiquidLeakingCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(LiquidLeakingCalculator::new());
        assert_eq!(calc.name(), "LiquidLeakingCalculator");
    }
}
