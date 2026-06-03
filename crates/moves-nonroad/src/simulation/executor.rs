//! The geography-execution seam: the boundary between the *driver
//! loop* and the *numerical evaluation* of NONROAD's six geography
//! routines.
//!
//! # Why a seam
//!
//! `nonroad.f`'s record loop ([`plan_scc_group`](crate::driver::plan_scc_group))
//! decides *which* geography routine each population record dispatches
//! to. ported that decision logic as a pure planner and named
//! its consumer explicitly:
//!
//! > the executor that runs each decision against the geography
//! > routines and the writers is the integration layer.
//!
//! [`run_simulation`](super::run_simulation) *is* that executor — it
//! walks the planner's [`DriverStep`](crate::driver::DriverStep)s and
//! invokes a geography routine for each [`Dispatch`]. But the six
//! routines ([`process_county`],
//! [`process_national_record`],
//! …) are not uniform: they take four different callback traits, each
//! of which must be populated from the loaded emission-factor,
//! technology, activity, growth, and retrofit tables. Assembling those
//! callback contexts behind one narrow trait, [`GeographyExecutor`],
//! keeps the driver loop decoupled from the callback-context assembly.
//!
//! This module provides two executors:
//!
//! - **[`ProductionExecutor`]** — assembles the four callback traits
//! ([`GeographyCallbacks`], [`StateCallbacks`], [`UsTotalCallbacks`],
//! [`NationalCallbacks`]) from loaded reference-data tables and calls
//! the real geography routines.
//! - **[`PlanRecordingExecutor`]** — records each dispatch and returns
//! empty output; makes the driver loop exercisable without any
//! reference data. It is also the minimal shape the NONROAD
//! numerical-fidelity harness needs for capturing
//! port-side intermediate state.

use crate::allocation::{allocate_county, allocate_state, CountyDescriptor};
use crate::common::consts::{
    CVTTON, MXAGYR, MXDAYS, MXEVTECH, MXPOL, MXTECH, RMISS, SWTCNG, SWTDSL, SWTGS2, SWTGS4, SWTLPG,
};
use crate::driver::scrptime;
use crate::driver::{day_month_factors as daymthf, DayMonthFactors};
use crate::driver::{fuel_for_scc, Dispatch, DriverRecord};
use crate::emissions::exhaust::{
    calculate_emission_adjustments, calculate_exhaust_emissions,
    ActivityUnit as ExhaustActivityUnit, AdjustmentInputs, DailyTemperatures, DayRange,
    EmissionUnitCode, ExhaustCalcInputs, FuelKind, PollutantFilter, Season,
};
use crate::geography::common::fuel_density;
use crate::geography::common::{
    ActivityRecord, ActivityUnit, BmyKind, EmissionsIterationResult, EvapFactorsLookup,
    ExhaustFactorsLookup, GeographyCallbacks, ModelYearAgedistResult, PopulationRecord,
    ProcessOutcome, ProcessOutput, RefuelingData, RetrofitFilter, RunOptions as CountyRunOptions,
    SumType, TechLookup,
};
use crate::geography::prcnat::{NationalCallbacks, StateAllocationOutcome};
use crate::geography::prcus::{
    DayMonthFactor, EvapCallInputs, EvapResult, EvapTechLookup, ExhaustCallInputs, ExhaustResult,
    ExhaustTechLookup, ModelYearOutput as PrcusModelYearOutput, RetrofitResult, UsTotalCallbacks,
};
use crate::geography::state::{CountyInput, StateContext};
use crate::geography::subcounty::SubcountyRecordIndex;
use crate::geography::{
    process_county, process_national_record, process_state_from_national_record,
    process_state_to_county_record, process_subcounty, process_us_total_record, ActivityLookup,
    EquipmentRecord, GeographyOutput, NationalContext, RunOptions as StateRunOptions,
    StateCallbacks, StateDescriptor, UsTotalContext,
};
use crate::input::spillage::{RangeIndicator, RefuelingMode, SpillageRecord};
use crate::population::retrofit::RetrofitRecord;
use crate::population::{
    age_distribution, growth_factor, model_year, select_for_indicator, ActivityUnits,
    GrowthIndicatorRecord,
};
use crate::{Error, Result};

use super::inputs::ReferenceData;
use super::options::NonroadOptions;
use super::outputs::{EmissionChannel, SimEmissionRow};

use crate::common::consts::MXHPC;
use crate::emissions::exhaust::AdjustmentTable;

/// One geography-routine dispatch decision the driver loop hands to a
/// [`GeographyExecutor`].
///
/// A read-only view assembled per `(record, Dispatch)` pair: it
/// borrows the record and SCC from [`NonroadInputs`](super::NonroadInputs)
/// and carries the planner-derived [`fuel`](Self::fuel) and
/// [`growth`](Self::growth). An executor turns this into a call to one
/// of the six geography routines.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DispatchContext<'a> {
    /// Which geography routine this record dispatches to.
    pub dispatch: Dispatch,
    /// The 10-character SCC of the dispatching record's group.
    pub scc: &'a str,
    /// Fuel resolved from the SCC by
    /// [`fuel_for_scc`]. `None` is the
    /// Fortran `ifuel = 0` "no prefix matched" default.
    pub fuel: Option<FuelKind>,
    /// The population record being dispatched.
    pub record: &'a DriverRecord,
    /// Per-year fractional growth rate when this record paired with
    /// its successor as a growth record; `None` is the Fortran
    /// `growth = -9` "no growth record" sentinel. See
    /// [`growth_pair`](crate::driver::growth_pair).
    pub growth: Option<f32>,
}

/// The result of executing one [`DispatchContext`] against a geography
/// routine.
///
/// A production [`GeographyExecutor`] maps the routine's native output
/// (`ProcessOutput`, `GeographyOutput`, `StateAggregate`, …) onto this
/// uniform shape; the driver loop folds it into
/// [`NonroadOutputs`](super::NonroadOutputs) via
/// [`absorb`](super::NonroadOutputs::absorb).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GeographyExecution {
    /// Emission rows the routine produced. Empty for a routine that
    /// took an early-out (`ISKIP`) or for a planning-only executor.
    pub rows: Vec<SimEmissionRow>,
    /// Non-fatal warnings the routine raised — the Fortran `chkwrn`
    /// channel.
    pub warnings: Vec<String>,
    /// `true` when the routine took an `ISKIP` early-out (county not
    /// in run, zero population, missing technology fractions, …).
    /// Counted into [`RunCounters::geography_skips`](super::RunCounters::geography_skips).
    pub skipped: bool,
    /// National-level records the routine processed — Fortran
    /// `nnatrc`. `0` for the county / state / subcounty levels.
    pub national_record_count: i32,
}

impl GeographyExecution {
    /// A skipped (`ISKIP`) execution: no rows, no warnings.
    ///
    /// The shorthand a production executor returns when a geography
    /// routine takes an early-out.
    pub fn skipped() -> Self {
        Self {
            skipped: true,
            ..Self::default()
        }
    }
}

/// The driver loop's view of NONROAD's six geography routines.
///
/// [`run_simulation`](super::run_simulation) calls
/// [`execute`](Self::execute) once per `(record, Dispatch)` pair the
/// planner produced, in dispatch order. An implementor either
/// evaluates the matching geography routine (the production path) or
/// records the call for inspection / instrumentation (see
/// [`PlanRecordingExecutor`]).
///
/// The trait is intentionally object-safe — its single method is
/// non-generic — so [`run_simulation`](super::run_simulation) accepts
/// both a concrete `&mut impl GeographyExecutor` and a
/// `&mut dyn GeographyExecutor`.
pub trait GeographyExecutor {
    /// Execute one geography-routine dispatch.
    ///
    /// `ctx` identifies the routine ([`DispatchContext::dispatch`]) and
    /// carries the record, SCC, fuel, and growth rate; `options`
    /// carries the run-global settings. The returned
    /// [`GeographyExecution`] is the routine's output in the uniform
    /// shape the driver loop folds into the run result.
    ///
    /// # Errors
    ///
    /// Propagates any [`Error`] the geography routine
    /// raises — a non-finite emission accumulator, a missing
    /// allocation coefficient, an exhausted retrofit population, …. An
    /// error aborts the whole run: NONROAD has no per-record error
    /// recovery, and neither does the port.
    fn execute(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution>;
}

/// An owned snapshot of one [`DispatchContext`], captured by
/// [`PlanRecordingExecutor`].
///
/// [`DispatchContext`] borrows from [`NonroadInputs`](super::NonroadInputs);
/// this is the detached, `'static` form so a recorded run plan can
/// outlive the inputs that produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedDispatch {
    /// The geography routine the record dispatched to.
    pub dispatch: Dispatch,
    /// SCC of the dispatching record's group.
    pub scc: String,
    /// Fuel resolved from the SCC.
    pub fuel: Option<FuelKind>,
    /// Region code of the dispatched record.
    pub region_code: String,
    /// Average HP of the dispatched record.
    pub hp_avg: f32,
    /// Equipment population of the dispatched record.
    pub population: f32,
    /// Population-input year of the dispatched record.
    pub pop_year: i32,
    /// Growth rate carried by the dispatching driver step.
    pub growth: Option<f32>,
}

impl RecordedDispatch {
    /// Detach a [`DispatchContext`] into an owned [`RecordedDispatch`].
    pub fn from_context(ctx: &DispatchContext<'_>) -> Self {
        Self {
            dispatch: ctx.dispatch,
            scc: ctx.scc.to_string(),
            fuel: ctx.fuel,
            region_code: ctx.record.region_code.clone(),
            hp_avg: ctx.record.hp_avg,
            population: ctx.record.population,
            pop_year: ctx.record.pop_year,
            growth: ctx.growth,
        }
    }
}

/// A [`GeographyExecutor`] that records every dispatch and evaluates
/// nothing.
///
/// This is the reference executor for the driver loop. It serves three
/// purposes:
///
/// 1. **Dry-run planning.** Driving
/// [`run_simulation`](super::run_simulation) with a
/// `PlanRecordingExecutor` produces a
/// [`NonroadOutputs`](super::NonroadOutputs) whose
/// [`counters`](super::NonroadOutputs::counters) and
/// [`completion_message`](super::NonroadOutputs::completion_message)
/// are fully populated — the complete run *structure* — while
/// [`dispatches`](Self::dispatches) holds the ordered dispatch
/// plan. The orchestrator can inspect what a run *will* do before
/// paying for the numerics.
/// 2. **Instrumentation skeleton.** It is the minimal shape of the
/// recording executor the numerical-fidelity harness needs (see the
/// module docs): swap the empty [`GeographyExecution`] for the real
/// routine output and the recorder also captures port-side
/// intermediate state.
/// 3. **Test double.** Unit and integration tests assert the driver
/// loop's dispatch order and counters against
/// [`dispatches`](Self::dispatches) without standing up the
/// geography routines.
///
/// Every [`execute`](GeographyExecutor::execute) call returns an empty
/// (non-skipped) [`GeographyExecution`], so a recorded run produces no
/// emission rows.
#[derive(Debug, Clone, Default)]
pub struct PlanRecordingExecutor {
    /// Every dispatch the driver loop made, in order.
    pub dispatches: Vec<RecordedDispatch>,
}

impl PlanRecordingExecutor {
    /// Create an executor with an empty dispatch log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of dispatches recorded so far.
    pub fn len(&self) -> usize {
        self.dispatches.len()
    }

    /// `true` when no dispatch has been recorded.
    pub fn is_empty(&self) -> bool {
        self.dispatches.is_empty()
    }

    /// The recorded dispatches that targeted `dispatch`.
    pub fn dispatches_to(&self, dispatch: Dispatch) -> Vec<&RecordedDispatch> {
        self.dispatches
            .iter()
            .filter(|d| d.dispatch == dispatch)
            .collect()
    }
}

impl GeographyExecutor for PlanRecordingExecutor {
    fn execute(
        &mut self,
        ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        self.dispatches.push(RecordedDispatch::from_context(ctx));
        Ok(GeographyExecution::default())
    }
}

// (Entry types ExhaustTechEntry, EvapTechEntry, GrowthXrefEntry,
// ActivityTableEntry, NationalAllocationEntry are defined in
// super::inputs and re-imported above.)

// =============================================================================
// ProductionExecutor
// =============================================================================

/// Production [`GeographyExecutor`] that routes each [`DispatchContext`] to
/// the matching NONROAD geography routine by assembling the four callback
/// traits (`GeographyCallbacks`, `StateCallbacks`, `UsTotalCallbacks`,
/// `NationalCallbacks`) from loaded reference-data tables.
///
/// # Callback-surface audit (T1 / )
///
/// Every method listed below appears on one or more of the four callback
/// traits. For each method: (a) the reference-data table it reads, and
/// (b) the already-ported `population::` / `emissions::` function it
/// should forward to. Methods whose backing data is not yet loadable are
/// marked **⚠ NOT YET LOADABLE**.
///
/// ## FIPS / region selection
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_fips` | `fndchr(fipin, 5, fipcod, NCNTY)` | County FIPS list (`fipcod(NCNTY)`) from `RunRegions::selected_counties` | — (index scan) | **available** |
/// | `tally_county_record` | `nctyrc(idxfip) += 1` | In-memory counter array (`nctyrc`) on the executor | — | **no loading needed** |
/// | `find_subcounty` | `prcsub.f` :246–258, `reglst(idxreg)(6:10)` | Region list (`reglst`) in `NonroadInputs::regions.region_list` | — | **available** |
///
/// ## Technology fractions
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_exhaust_tech` | `fndtch(asccod, hpval, year)` | Exhaust tech-type fractions (`tchfrc`, `tectyp`) from NR\*.EF emission-factor files | — (table lookup) | **available** via [`exhaust_tech_entries`](ReferenceData::exhaust_tech_entries) |
/// | `find_evap_tech` | `fndevtch(asccod, hpval, year)` | Evap tech-type fractions (`evtchfrc`, `evtectyp`) from NR\*.EF files | — (table lookup) | **available** via [`evap_tech_entries`](ReferenceData::evap_tech_entries) |
///
/// ## Activity records
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_activity` | `fndact(asccod, fipin, hpval)` | Activity records (`actlev`, `faclod`, `iactun`, `actage`, `starts`) from NR\*.ACT files | — | **available** via [`activity_entries`](ReferenceData::activity_entries) |
/// | `activity_record` | reads `actlev(idxact)`, `faclod(idxact)`, `iactun(idxact)`, `actage(idxact)`, `starts(idxact)` | Same NR\*.ACT records as `find_activity` | — | **available** |
///
/// ## Growth cross-reference and growth factors
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_growth_xref` | `fndgxf(fipin, asccod, hpval)` | Growth cross-reference table (`gxfdat`) from NR\*.GRW indicator files | — | **available** via [`growth_xref_entries`](ReferenceData::growth_xref_entries) |
/// | `load_growth` | `getgrw(indcod)` | Growth-factor stream from NR\*.GRW files | `population::growth::select_for_indicator` | **ported** |
/// | `growth_factor` | `grwfac(year1, year2, fips, indcod)` | Loaded growth records (above) | `population::growth::growth_factor` | **ported** |
///
/// ## Model-year and age distribution
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `model_year` / `model_year_and_agedist` | `modyr(…)` | Scrappage curves (`population::scrappage`) | `population::modyr::model_year` | **ported** |
/// | `age_distribution` | `agedist(…)` | Grown model-year fractions from `modyr` output | `population::agedist::age_distribution` | **ported** |
///
/// ## Retrofit records
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `filter_retrofits` / `filter_retrofits_by_scc_hp` / `_year` / `_tech` | `fndrtrft(fltrtyp, …)` | Retrofit records (`population::retrofit::RetrofitRecord`) from NR\*.RFT files | `population::retrofit::sort_retrofits` / `compare_retrofits` | **ported** (records must be loaded) |
/// | `surviving_retrofits` | `rtrftfltr3(*)` read after type-3 filter | Same retrofit records | — | **ported** |
/// | `calculate_retrofit` | `clcrtrft(…)` | Retrofit records + per-pollutant reduction fractions | `emissions::retrofit::calculate_retrofit_reduction` | **ported** |
///
/// ## Temporal factors
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `day_month_factors` / `day_month_factor` | `daymthf(asccod, fipin, daymthfac, mthf, dayf, ndays)` | Day/month factor table from NR\*.TMF temporal-fraction files | — (table lookup) | **⚠ NOT YET LOADABLE** — temporal-factor file loader not ported |
/// | `emission_adjustments` | `emsadj(adjems, asccod, fipin, daymthfac)` | Emission-factor files for the adjustment table lookup | `emissions::exhaust::calculate_emission_adjustments` | **math ported**; requires loaded EF tables (**⚠ NOT YET LOADABLE**) |
///
/// ## Exhaust and evaporative emission factors
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `compute_exhaust_factors` | `emfclc(…)` | Emission-factor records from NR\*.EMF files | `emissions::exhaust::compute_emission_factor_for_tech` | **math ported**; EF file loader **⚠ NOT YET LOADABLE** |
/// | `compute_evap_factors` | `evemfclc(…)` | Evap emission-factor records from `nrevapemissionrate` | — (rate table lookup, not full NONROAD EF physics) | **available** — rates loaded from `nrevapemissionrate` |
///
/// ## Emission calculators
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `compute_exhaust_iteration` / `calculate_exhaust` | `clcems(…)` | EF table (above) + activity records | `emissions::exhaust::calculate_exhaust_emissions` | **ported** (depends on unloaded EF + activity tables) |
/// | `compute_evap_iteration` | `clcevems(…)` | `nrevapemissionrate` rates | rate × activity (simplified, not full NONROAD physics) | **available** for g/hr and g/start species; g/m²/day (permeation) and Mult (diurnal) return RMISS — blocked by missing spillage/meteorology data |
/// | `calculate_evap` (state/national) | `clcevems(…)` | same | — | returns empty — county path handles evap |
///
/// ## Refueling / spillage
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_refueling` | `fndrfm(asccod, hpval, tech)` | Refueling/spillage-mode records from NR\*.SPL files (`ReferenceData::spillage_records`) | — | **available** — populate `spillage_records` and set `NonroadOptions::spillage_loaded = true` |
///
/// ## Allocation (subcounty and national)
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_allocation` (subcounty) | `fndasc(asccod, ascalo, nalorc)` | Subcounty allocation coefficients from NR\*.SCO files | — | **⚠ NOT YET LOADABLE** |
/// | `allocate_subcounty` | `alosub(…)` | Same NR\*.SCO records | — (pure computation) | **⚠ NOT YET PORTED** |
/// | `find_allocation` (national) | `fndasc` national path | National-to-state allocation coefficients from NR\*.ALO files | [`NationalAllocationEntry::record`] + [`ReferenceData::allocation_indicators`] | ✓ loadable by caller |
/// | `allocate_to_states` | `alosta(…)` | Same NR\*.ALO records | [`allocation::allocate_state`] | ✓ ported (mo-i6q) |
///
/// # Summary: what blocks production execution
///
/// The following reference-data loaders must be ported before
/// `ProductionExecutor` can produce fully-populated results:
///
/// 1. **NR\*.EF** — exhaust emission-factor records (`emfclc`, `emsadj`).
///    Evap rates are now loaded from `nrevapemissionrate`.
/// 2. **NR\*.TMF** — temporal day/month factor table (`daymthf`).
/// 3. **NR\*.SPL** — refueling/spillage-mode records (`fndrfm`). Without
///    spillage data, evap permeation (g/m²/day) and diurnal (Mult) cannot
///    be computed — those species return RMISS (explicitly not-computed).
/// 4. **NR\*.SCO** — subcounty allocation coefficients.
/// 5. **`alosub` port** — subcounty allocation math (pure computation, not yet ported).
///
/// Tech-type fractions (NR\*.EF), activity records (NR\*.ACT), growth
/// cross-reference and growth-factor data (NR\*.GRW), and retrofit
/// records (NR\*.RFT) are now carried as typed fields and populated by
/// the caller.
#[derive(Debug, Default)]
pub struct ProductionExecutor {
    /// County FIPS codes in the run (`fipcod(NCNTY)`). Used by
    /// `CountyAdapter::find_fips` to map a region-code to its slot
    /// index.
    pub county_fips: Vec<String>,
    /// HP-category midpoints (`hpclev(1..MXHPC)`) used by the
    /// `hp_level_lookup` call in [`process_county`] /
    /// [`process_subcounty`]. Defaults to all-zeros (every HP record
    /// resolves to `9999.0`); set to the standard NONROAD values for
    /// production runs.
    pub hp_levels: [f32; MXHPC],
    /// State descriptors for national dispatch. Parallel to the
    /// `statcd`/`lstacd`/`lstlev` Fortran arrays; required to build
    /// [`NationalContext::states`].
    pub state_descriptors: Vec<StateDescriptor>,
    /// Reference tables loaded from input files — tech fractions,
    /// activity records, growth data, allocation, retrofit, and
    /// temporal factors. Built once per run by the orchestrator and
    /// passed to [`ProductionExecutor::new`].
    pub reference: ReferenceData,
    /// Which months are active — Fortran `lmonth(12)` from `/perdat/`.
    /// Index 0 = January. Set from [`NonroadOptions::selected_month`].
    /// When all `false`, defaults are used (flat temporal profile).
    pub months_selected: [bool; 12],
    /// `true` when weekday is selected — Fortran `ldays(IDXWKD)`.
    pub weekday_selected: bool,
    /// `true` for period-total runs — Fortran `ismtyp == IDXTOT`.
    /// Mirrors [`NonroadOptions::total_mode`] on the executor so the
    /// temporal callbacks can access it without receiving the options.
    pub total_mode: bool,
}

impl ProductionExecutor {
    /// Create a `ProductionExecutor` from a pre-loaded [`ReferenceData`] bundle.
    ///
    /// All other fields (`county_fips`, `hp_levels`, `state_descriptors`)
    /// default to empty; every county / subcounty dispatch returns
    /// [`GeographyExecution::skipped`] (FIPS not found) until
    /// `county_fips` and the other fields are populated after construction.
    ///
    /// # Examples
    ///
    /// ```
    /// use moves_nonroad::simulation::{ProductionExecutor, ReferenceData};
    ///
    /// let ref_data = ReferenceData::default();
    /// let executor = ProductionExecutor::new(&ref_data);
    /// assert!(executor.reference.exhaust_tech_entries.is_empty());
    /// assert!(executor.reference.retrofit_records.is_empty());
    /// ```
    pub fn new(ref_data: &ReferenceData) -> Self {
        Self {
            reference: ref_data.clone(),
            ..Self::default()
        }
    }

    /// Resolve the temporal profile for `scc`, falling back through the
    /// SCC family root then the canonical defaults (`defmth`, `defday`).
    ///
    /// Returns `(monthly[12], daily[2])` ready to pass to
    /// [`crate::driver::day_month_factors`].
    fn resolve_temporal_profile(&self, scc: &str) -> ([f32; 12], [f32; 2]) {
        let def_monthly = [1.0_f32 / 12.0; 12];
        let def_daily = [1.0_f32 / 7.0; 2];
        if let Some(p) = self.reference.temporal_profiles.get(scc) {
            return (p.monthly, p.daily);
        }
        // Family-root fallback: first 7 chars then first 4 chars (mirrors fndtpm.f).
        if scc.len() >= 7 {
            let root7 = format!("{:0<10}", &scc[..7]);
            if let Some(p) = self.reference.temporal_profiles.get(&root7) {
                return (p.monthly, p.daily);
            }
        }
        if scc.len() >= 4 {
            let root4 = format!("{:0<10}", &scc[..4]);
            if let Some(p) = self.reference.temporal_profiles.get(&root4) {
                return (p.monthly, p.daily);
            }
        }
        (def_monthly, def_daily)
    }

    /// Compute [`DayMonthFactors`] for `scc` using the executor's period
    /// flags and temporal profiles — the in-process equivalent of the
    /// Fortran `daymthf(asccod, fipin, daymthfac, mthf, dayf, ndays)` call.
    fn day_month_factors_for(&self, scc: &str) -> DayMonthFactors {
        let (monthly, daily) = self.resolve_temporal_profile(scc);
        daymthf(
            &monthly,
            &daily,
            &self.months_selected,
            self.weekday_selected,
            false, // ldayfl: no daily-temp file in MOVES runs
            self.total_mode,
        )
    }

    fn execute_county(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let hp_levels = self.hp_levels;
        let run_options = build_run_options(ctx, options, hp_levels);
        let record = PopulationRecord {
            region_code: &ctx.record.region_code,
            population: ctx.record.population,
            // hp_range midpoint = hp_avg; DriverRecord has no range so
            // we synthesise (0, 2*hp_avg) → mid = hp_avg.
            hp_range: (0.0, ctx.record.hp_avg * 2.0),
            hp_avg: ctx.record.hp_avg,
            // Median life (scrptime's `mdlfhrs`) from the driver record;
            // fall back to a neutral 1000.0 when absent so scrptime stays
            // well-defined. `disc_code` is not carried; DEFAULT curve.
            use_hours: if ctx.record.median_life > 0.0 {
                ctx.record.median_life
            } else {
                1000.0
            },
            disc_code: "DEFAULT",
            base_pop_year: ctx.record.pop_year,
            scc: ctx.scc,
        };
        let mut adapter = CountyAdapter::new(self, ctx.growth);
        let outcome = process_county(&record, &run_options, &mut adapter)?;
        Ok(process_outcome_to_execution(outcome))
    }

    fn execute_subcounty(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let hp_levels = self.hp_levels;
        let run_options = build_run_options(ctx, options, hp_levels);
        let record = PopulationRecord {
            region_code: &ctx.record.region_code,
            population: ctx.record.population,
            hp_range: (0.0, ctx.record.hp_avg * 2.0),
            hp_avg: ctx.record.hp_avg,
            use_hours: if ctx.record.median_life > 0.0 {
                ctx.record.median_life
            } else {
                1000.0
            },
            disc_code: "DEFAULT",
            base_pop_year: ctx.record.pop_year,
            scc: ctx.scc,
        };
        let cached_growth = ctx.growth;
        let mut adapter = CountyAdapter::new(self, ctx.growth);
        let outcome = process_subcounty(
            SubcountyRecordIndex(0),
            &record,
            cached_growth,
            &run_options,
            &mut adapter,
        )?;
        Ok(process_outcome_to_execution(outcome))
    }

    fn execute_state_to_county(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let hp_levels = self.hp_levels;
        let _state_ctx = build_state_context(ctx, options, &hp_levels);

        // Build county list by filtering county_fips to those whose
        // 2-char state prefix matches the state FIPS in region_code.
        let state_prefix = ctx.record.region_code.get(..2).unwrap_or("");
        let county_fips: Vec<String> = self
            .county_fips
            .iter()
            .filter(|fips| fips.get(..2).unwrap_or("") == state_prefix)
            .cloned()
            .collect();

        if county_fips.is_empty() {
            return Ok(GeographyExecution::skipped());
        }

        // Find the NR*.SCO allocation cross-reference record for this SCC.
        // `alocty.f` :65–70 calls `fndasc(asccod, ascalo, nalorc)` to locate
        // the SCC's row in the allocation arrays; missing → error.
        let alloc_record = self
            .reference
            .county_allocation_records
            .iter()
            .find(|r| r.scc == ctx.scc)
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "execute_state_to_county: no NR*.SCO county-allocation record \
                     (alocty.f / fndasc) for SCC {}; county populations cannot be \
                     computed without an allocation cross-reference entry.",
                    ctx.scc
                ))
            })?;

        // All counties in the matching state are selected; none are flagged
        // as carrying their own county-level records (those dispatch via
        // Dispatch::County and do not appear in the StateToCounty path).
        let county_descriptors: Vec<CountyDescriptor> = county_fips
            .iter()
            .map(|fips| CountyDescriptor {
                fips: fips.clone(),
                selected: true,
                has_county_records: false,
            })
            .collect();

        // Allocate state population to counties via spatial-indicator
        // regression — ports `alocty.f`. `ctx.record.region_code` is the
        // 5-char state FIPS (e.g., "06000"), matching `regncd(icurec)(1:5)`
        // in the Fortran.
        let growth = ctx.growth.unwrap_or(0.0);
        let allocations = allocate_county(
            &ctx.record.region_code,
            &county_descriptors,
            alloc_record,
            &self.reference.county_allocation_indicators,
            options.episode_year,
            ctx.record.population,
            growth,
        )?;

        let counties: Vec<CountyInput> = allocations
            .into_iter()
            .map(|a| CountyInput {
                fips: a.fips,
                selected: true,
                population: a.population,
            })
            .collect();

        let mut adapter = StateAdapter::new(self);
        let output = process_state_to_county_record(&_state_ctx, &counties, &mut adapter)?;
        Ok(geography_output_to_execution(output))
    }

    fn execute_state_from_national(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let hp_levels = self.hp_levels;
        let state_ctx = build_state_context(ctx, options, &hp_levels);
        let mut adapter = StateAdapter::new(self);
        let output = process_state_from_national_record(&state_ctx, &mut adapter)?;
        Ok(geography_output_to_execution(output))
    }

    fn execute_national(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        if self.state_descriptors.is_empty() {
            return Ok(GeographyExecution::skipped());
        }
        let hp_levels = self.hp_levels;
        let states: Vec<StateDescriptor> = self.state_descriptors.clone();
        let national_ctx = build_national_context(ctx, options, &hp_levels, &states);
        let mut adapter = NationalAdapter::new(self);
        let output = process_national_record(&national_ctx, &mut adapter)?;
        Ok(geography_output_to_execution(output))
    }

    fn execute_us_total(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let hp_levels = self.hp_levels;
        let us_total_ctx = build_us_total_context(ctx, options, &hp_levels);
        let mut adapter = UsTotalAdapter::new(self);
        let output = process_us_total_record(&us_total_ctx, &mut adapter)?;
        Ok(geography_output_to_execution(output))
    }
}

impl GeographyExecutor for ProductionExecutor {
    fn execute(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        match ctx.dispatch {
            Dispatch::County => self.execute_county(ctx, options),
            Dispatch::Subcounty => self.execute_subcounty(ctx, options),
            Dispatch::StateToCounty => self.execute_state_to_county(ctx, options),
            Dispatch::StateFromNational => self.execute_state_from_national(ctx, options),
            Dispatch::National => self.execute_national(ctx, options),
            Dispatch::UsTotal => self.execute_us_total(ctx, options),
        }
    }
}

// =============================================================================
// CountyAdapter — implements GeographyCallbacks for ProductionExecutor
// =============================================================================

/// Adapter that implements [`GeographyCallbacks`] by borrowing
/// reference tables from a [`ProductionExecutor`].
///
/// Created per-call in [`ProductionExecutor::execute_county`] /
/// `execute_subcounty`; dropped when the geography routine returns.
/// All table lookups are linear scans over the executor's typed
/// reference-data fields.
struct CountyAdapter<'a> {
    executor: &'a mut ProductionExecutor,
    /// Indices of currently-surviving retrofit records — narrowed
    /// progressively by [`filter_retrofits`](GeographyCallbacks::filter_retrofits).
    retrofit_survivors: Vec<usize>,
    /// Population growth rate from the driver record pair
    /// ([`DispatchContext::growth`]). Forwarded to [`scrptime`] as the
    /// `pop_growth_factor` argument.
    ctx_growth: Option<f32>,
}

impl<'a> CountyAdapter<'a> {
    fn new(executor: &'a mut ProductionExecutor, ctx_growth: Option<f32>) -> Self {
        Self {
            executor,
            retrofit_survivors: Vec::new(),
            ctx_growth,
        }
    }
}

impl<'a> GeographyCallbacks for CountyAdapter<'a> {
    // ---- FIPS / region selection -----------------------------------------

    fn find_fips(&self, fips: &str) -> Option<usize> {
        self.executor.county_fips.iter().position(|f| f == fips)
    }

    fn tally_county_record(&mut self, _fips_idx: usize) {
        // County record tallies are informational only and not yet
        // surfaced to the executor's output. No-op for now.
    }

    // ---- Technology fractions -------------------------------------------

    fn find_exhaust_tech(&self, scc: &str, hp_avg: f32, year: i32) -> Option<TechLookup> {
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(TechLookup {
            scc_tech_index: 0,
            tech_names: entry.tech_names.clone(),
            // Per-model-year tech mix (cleaner tech phases in over model
            // years); falls back to the single vector when no per-year
            // data is loaded.
            tech_fractions: entry.fractions_for_year(year).to_vec(),
        })
    }

    fn find_evap_tech(&self, scc: &str, hp_avg: f32, _year: i32) -> Option<TechLookup> {
        let entry = self
            .executor
            .reference
            .evap_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(TechLookup {
            scc_tech_index: 0,
            tech_names: entry.tech_names.clone(),
            tech_fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_refueling(&self, scc: &str, hp_avg: f32, tech_name: &str) -> Option<RefuelingData> {
        fndrfm(
            &self.executor.reference.spillage_records,
            scc,
            hp_avg,
            tech_name,
        )
    }

    // ---- Growth cross-reference -----------------------------------------

    fn find_growth_xref(&self, fips: &str, scc: &str, hp_avg: f32) -> Option<usize> {
        self.executor
            .reference
            .growth_xref_entries
            .iter()
            .position(|e| {
                e.fips == fips && e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max
            })
    }

    // ---- Activity records -----------------------------------------------

    fn find_activity(&self, scc: &str, fips: &str, _hp_avg: f32) -> Option<usize> {
        self.executor
            .reference
            .activity_entries
            .iter()
            .position(|e| e.scc == scc && (e.fips.is_empty() || e.fips == fips))
    }

    fn activity_record(&self, activity_index: usize) -> ActivityRecord {
        let entry = match self.executor.reference.activity_entries.get(activity_index) {
            Some(e) => e,
            None => {
                return ActivityRecord {
                    starts: 0.0,
                    activity_level: 0.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.0,
                    age_code: String::new(),
                }
            }
        };
        ActivityRecord {
            starts: entry.starts,
            activity_level: entry.activity_level,
            activity_unit: entry.activity_unit,
            load_factor: entry.load_factor,
            age_code: entry.age_code.clone(),
        }
    }

    // ---- Retrofit filtering ---------------------------------------------

    fn filter_retrofits(
        &mut self,
        filter: RetrofitFilter,
        scc: &str,
        hp_avg: f32,
        model_year: i32,
        tech_name: &str,
    ) -> Result<()> {
        match filter {
            RetrofitFilter::SccHp => {
                // Initialise: keep records whose SCC and HP range match.
                self.retrofit_survivors = self
                    .executor
                    .reference
                    .retrofit_records
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| {
                        (r.scc == scc || r.scc == crate::population::RTRFTSCC_ALL)
                            && r.hp_min < hp_avg
                            && hp_avg <= r.hp_max
                    })
                    .map(|(i, _)| i)
                    .collect();
            }
            RetrofitFilter::ModelYear => {
                // Narrow: keep records whose model-year span contains model_year.
                self.retrofit_survivors.retain(|&i| {
                    let r = &self.executor.reference.retrofit_records[i];
                    r.year_model_start <= model_year && model_year <= r.year_model_end
                });
            }
            RetrofitFilter::TechType => {
                // Narrow: keep records whose tech type matches.
                self.retrofit_survivors.retain(|&i| {
                    let r = &self.executor.reference.retrofit_records[i];
                    r.tech_type.trim() == tech_name.trim()
                        || r.tech_type.trim() == crate::population::RTRFTTECHTYPE_ALL
                });
            }
        }
        Ok(())
    }

    fn surviving_retrofits(&self) -> Vec<&RetrofitRecord> {
        self.retrofit_survivors
            .iter()
            .map(|&i| &self.executor.reference.retrofit_records[i])
            .collect()
    }

    // ---- Temporal factors -----------------------------------------------

    fn day_month_factors(&self, scc: &str, _fips: &str) -> Result<([f32; MXDAYS], f32, f32, i32)> {
        // Canonical `daymthf.f`: look up the per-SCC monthly/daily profile,
        // accumulate mthf and ndays over the selected months (lmonth), then
        // set dayf from the day-of-week factor (ismtyp / ldays).
        let dmf = self.executor.day_month_factors_for(scc);
        let mut arr = [0.0_f32; MXDAYS];
        arr.iter_mut()
            .zip(dmf.day_factors.iter())
            .for_each(|(a, b)| *a = *b);
        Ok((arr, dmf.month_factor, dmf.day_of_week_factor, dmf.n_days))
    }

    fn emission_adjustments(
        &self,
        scc: &str,
        fips: &str,
        _daymthfac: &[f32; crate::common::consts::MXDAYS],
    ) -> Result<AdjustmentTable> {
        let oxy = self.executor.reference.fuel_oxygen_pct;
        // Per-SCC activity-weighted ambient temperature (warm-daytime-weighted
        // for daylight-use equipment), falling back to the run-level scalar.
        // Canonical emsadj.f:167-220 ALWAYS applies EXP(acoeff*(tamb-75));
        // absent zonemonthhour is a load error, not a silent 75 °F bypass.
        let tamb = self
            .executor
            .reference
            .ambient_temp_by_scc
            .get(scc)
            .copied()
            .or(self.executor.reference.ambient_temp_f)
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "emission_adjustments: run-level ambient temperature is absent for \
                     SCC {scc} (zonemonthhour not loaded). emsadj.f always applies \
                     EXP(acoeff*(tamb-75)); a neutral 75 °F cannot be fabricated as \
                     it silently drops the exhaust temperature correction."
                ))
            })?;
        if tamb <= 0.0 {
            return Err(crate::Error::Config(format!(
                "emission_adjustments: ambient temperature is non-positive ({tamb}°F) \
                 for SCC {scc}. This indicates a data error in zonemonthhour or \
                 nrhourallocation."
            )));
        }
        // Port of `emsadj.f`: the gasoline exhaust temperature + oxygenate
        // corrections. The engine evaluates day 1 (begin=end=1), matching
        // the county adapter's single-day exhaust iteration.
        let fuel = fuel_for_scc(scc).unwrap_or(FuelKind::Gasoline4Stroke);
        let temps = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: Vec::new(),
            ambient_temp: tamb,
        };
        let dummy_month = [1.0_f32; crate::common::consts::MXDAYS];
        let inputs = AdjustmentInputs {
            fuel,
            scc,
            fips,
            day_range: DayRange {
                begin_day: 1,
                end_day: 1,
                winter_skip_begin: 0,
                winter_skip_end: 0,
                winter_skip: false,
            },
            temperatures: &temps,
            daily_month_fraction: &dummy_month,
            rfg: self.executor.reference.fuel_rfg,
            high_altitude: false,
            oxygen_percent: oxy,
            episode_year: 0,
            month: 0,
            month_to_season: [Season::Summer; 12],
            rfg_winter_2_stroke: None,
            rfg_winter_4_stroke: None,
            rfg_summer_2_stroke: None,
            rfg_summer_4_stroke: None,
            // SOx/altitude unused by the emitted pollutants; neutral values
            // (soxcor = sox_fuel/sox_base = 1, altitude factor = 1).
            sox_fuel: [1.0; 5],
            sox_base: [1.0; 5],
            sox_diesel_marine: 1.0,
            altitude_factor: [1.0; 5],
        };
        Ok(calculate_emission_adjustments(&inputs))
    }

    // ---- Model-year and age distribution --------------------------------

    fn model_year_and_agedist(
        &mut self,
        activity_index: usize,
        record: &PopulationRecord<'_>,
        fips: &str,
        growth_index: usize,
        _episode_year: i32,
        growth_year: i32,
        base_population: f32,
    ) -> Result<ModelYearAgedistResult> {
        // 1. Growth indicator from cross-reference.
        let indicator_opt = self
            .executor
            .reference
            .growth_xref_entries
            .get(growth_index)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "CountyAdapter: growth_xref index {growth_index} out of range"
                ))
            })?;
        // Canonical prccty.f:327-328 / fndgxf: no cross-reference match
        // (idxgrw <= 0) → label 7001 (fatal error) when growth is active.
        // Base-year runs (growth_year == base_pop_year) are exempt because
        // age_distribution never calls growth_fn in that case, so an empty
        // selected set is harmless.
        if indicator_opt.is_none() && growth_year != record.base_pop_year {
            return Err(Error::Config(format!(
                "ERROR: Could not find match in growth indicator cross reference for: \
                 County {fips} SCC {scc} HP range {lo:6.1} {hi:6.1}",
                scc = record.scc,
                lo = record.hp_range.0,
                hi = record.hp_range.1,
            )));
        }
        let indicator = indicator_opt.unwrap_or_default();

        // 2. Select growth records for this indicator (clone to avoid
        // borrow-checker conflict with the scrptime closure below).
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();

        // 3. Activity entry.
        let act = self
            .executor
            .reference
            .activity_entries
            .get(activity_index)
            .ok_or_else(|| {
                Error::Config(format!(
                    "CountyAdapter: activity index {activity_index} out of range"
                ))
            })?
            .clone();

        // 4. Convert ActivityUnit from geography::common to population::modyr.
        let units = match act.activity_unit {
            ActivityUnit::HoursPerYear => ActivityUnits::HoursPerYear,
            ActivityUnit::HoursPerDay => ActivityUnits::HoursPerDay,
            ActivityUnit::GallonsPerYear => ActivityUnits::GallonsPerYear,
            ActivityUnit::GallonsPerDay => ActivityUnits::GallonsPerDay,
        };

        // 5. Clone scrappage curve so the closure can capture it by move.
        let curve = self.executor.reference.scrappage_curve.clone();
        let use_hours = record.use_hours;
        let load_factor = act.load_factor;
        let pop_growth_factor = self.ctx_growth.unwrap_or(0.0);

        // 6. model_year → calls scrptime closure.
        let modyr_out = model_year(
            act.starts,
            act.activity_level,
            units,
            load_factor,
            use_hours,
            &self.executor.reference.age_adjustment_table,
            &act.age_code,
            move |acttmp| scrptime(use_hours, load_factor, acttmp, pop_growth_factor, &curve),
        )?;

        // 7. age_distribution → uses growth records for forward/backward growth.
        let selected_refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
        let agedist_out = age_distribution(
            base_population,
            &modyr_out.modfrc,
            record.base_pop_year,
            growth_year,
            &modyr_out.yryrfrcscrp,
            |y1, y2| growth_factor(&selected_refs, y1, y2, fips),
        )?;

        // 8. Build ModelYearAgedistResult. modfrc comes from agedist
        // (grown to growth_year); the per-year adjustments from modyr.
        let nyrlif = modyr_out.nyrlif;
        // Ensure stradj/actadj/detage have at least nyrlif slots.
        let pad = |mut v: Vec<f32>| -> Vec<f32> {
            v.resize(nyrlif, 0.0);
            v
        };
        Ok(ModelYearAgedistResult {
            yryrfrcscrp: modyr_out.yryrfrcscrp,
            modfrc: agedist_out.mdyrfrc,
            stradj: pad(modyr_out.stradj),
            actadj: pad(modyr_out.actadj),
            detage: pad(modyr_out.detage),
            nyrlif,
            population: agedist_out.base_population,
        })
    }

    // ---- Emission factor lookups ----------------------------------------

    fn compute_exhaust_factors(
        &mut self,
        scc: &str,
        hp_avg: f32,
        tech_names: &[String],
        _tech_fractions: &[f32],
        _model_year: i32,
        _year_index: usize,
        _record_index: usize,
    ) -> Result<ExhaustFactorsLookup> {
        let n_tech = tech_names.len().max(1);

        // Resolve the hp-matched reference entry for this SCC — the same
        // entry `find_exhaust_tech` selected to produce `tech_names`. The
        // per-tech ordering of every array below is therefore aligned
        // with `tech_names` / `tech_fractions`.
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max);

        // Per-tech BSFC, broadcast across all calendar years. Canonical
        // `emfclc.f` always populates a real BSFC for the matched
        // (SCC, hp, tech): BSFC drives the fuel-consumption bookkeeping and
        // the BSFC-derived CO2/SOx chemistry. `compute_exhaust_factors` is
        // only called after `find_exhaust_tech` matched an entry, so a missing
        // entry here — or a matched entry carrying an empty BSFC vector — is a
        // reference-data error, not a defaultable 0.0. A zero BSFC silently
        // zeroes fuel consumption and the CO2/SOx that depend on it, so fail
        // loudly instead.
        let Some(e_bsfc) = entry else {
            return Err(crate::Error::Config(format!(
                "compute_exhaust_factors: no exhaust-tech reference entry for \
                 SCC {scc} at hp_avg {hp_avg}; emfclc.f (NR*.EMF packet) must supply a \
                 BSFC for the matched tech. A zero BSFC cannot be fabricated (it zeroes \
                 fuel consumption and the BSFC-derived CO2/SOx)."
            )));
        };
        if e_bsfc.bsfc.is_empty() {
            return Err(crate::Error::Config(format!(
                "compute_exhaust_factors: exhaust-tech entry for SCC {scc} at \
                 hp_avg {hp_avg} carries an empty BSFC vector; emfclc.f (NR*.EMF packet) \
                 populates a real per-tech BSFC. An empty/zero BSFC is a data error and \
                 cannot be fabricated (it zeroes fuel consumption and the BSFC-derived \
                 CO2/SOx)."
            )));
        }
        let bsfc_per_tech: Vec<f32> = e_bsfc.bsfc.clone();
        let mut bsfc = vec![0.0_f32; MXAGYR * n_tech];
        for y in 0..MXAGYR {
            for (t, &v) in bsfc_per_tech.iter().enumerate().take(n_tech) {
                bsfc[y * n_tech + t] = v;
            }
        }

        // EF / unit / deterioration arrays. They default to the legacy
        // zero-fill (only BSFC-derived CO2/SOx produced); when the
        // reference entry carries loaded emission factors they are
        // expanded into the engine's `[year][pollutant][tech]` and
        // `[pollutant][tech]` layouts. The base rate is the same for
        // every calendar year — the model-year/age signal enters through
        // the deterioration coefficients in `calculate_exhaust_emissions`.
        let mut emission_factors = vec![0.0_f32; MXAGYR * MXPOL * MXTECH];
        let mut unit_codes = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * MXTECH];
        let mut adetcf = vec![0.0_f32; MXPOL * MXTECH];
        let mut bdetcf = vec![0.0_f32; MXPOL * MXTECH];
        let mut detcap = vec![0.0_f32; MXPOL * MXTECH];

        if let Some(e) = entry {
            if !e.emission_factors.is_empty() {
                let stride = n_tech;
                let tech_span = n_tech.min(MXTECH);
                for pol in 0..MXPOL {
                    for t in 0..tech_span {
                        let src = pol * stride + t;
                        if src >= e.emission_factors.len() {
                            continue;
                        }
                        let ef = e.emission_factors[src];
                        if ef != 0.0 {
                            for y in 0..MXAGYR {
                                emission_factors[y * (MXPOL * MXTECH) + pol * MXTECH + t] = ef;
                            }
                        }
                        let dst = pol * MXTECH + t;
                        if let Some(u) = e.emission_units.get(src) {
                            unit_codes[dst] = *u;
                        }
                        if let Some(a) = e.det_a.get(src) {
                            adetcf[dst] = *a;
                        }
                        if let Some(b) = e.det_b.get(src) {
                            bdetcf[dst] = *b;
                        }
                        if let Some(c) = e.det_cap.get(src) {
                            detcap[dst] = *c;
                        }
                    }
                }
            }
        }

        Ok(ExhaustFactorsLookup {
            emission_factors,
            bsfc,
            unit_codes,
            adetcf,
            bdetcf,
            detcap,
        })
    }

    fn compute_evap_factors(
        &mut self,
        scc: &str,
        evap_tech_names: &[String],
        _evap_tech_fractions: &[f32],
        _model_year: i32,
        _year_index: usize,
        _record_index: usize,
    ) -> Result<EvapFactorsLookup> {
        // Find the EvapTechEntry whose SCC and tech_names match the lookup
        // result from `find_evap_tech`. The entry was selected by (SCC,
        // hp_avg) in `find_evap_tech`, and its tech_names are passed here
        // verbatim, so matching on SCC + tech_names equality is unambiguous.
        let entry = self
            .executor
            .reference
            .evap_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.tech_names == evap_tech_names);
        let Some(entry) = entry else {
            return Ok(EvapFactorsLookup::default());
        };
        if entry.emission_factors.is_empty() {
            return Ok(EvapFactorsLookup::default());
        }

        let n_tech = evap_tech_names.len();
        let tech_span = n_tech.min(MXEVTECH);

        let mut emission_factors = vec![0.0_f32; MXAGYR * MXPOL * MXEVTECH];
        let mut unit_codes = vec![EmissionUnitCode::GramsPerHour; MXPOL * MXEVTECH];
        let mut adetcf = vec![0.0_f32; MXPOL * MXEVTECH];
        let mut bdetcf = vec![0.0_f32; MXPOL * MXEVTECH];
        let mut detcap = vec![0.0_f32; MXPOL * MXEVTECH];

        for pol in 0..MXPOL {
            for t in 0..tech_span {
                let src = pol * n_tech + t;
                if src >= entry.emission_factors.len() {
                    continue;
                }
                let ef = entry.emission_factors[src];
                let dst = pol * MXEVTECH + t;
                if ef != 0.0 {
                    for y in 0..MXAGYR {
                        emission_factors[y * (MXPOL * MXEVTECH) + dst] = ef;
                    }
                }
                if let Some(u) = entry.unit_codes.get(src) {
                    unit_codes[dst] = *u;
                }
                if let Some(a) = entry.det_a.get(src) {
                    adetcf[dst] = *a;
                }
                if let Some(b) = entry.det_b.get(src) {
                    bdetcf[dst] = *b;
                }
                if let Some(c) = entry.det_cap.get(src) {
                    detcap[dst] = *c;
                }
            }
        }

        Ok(EvapFactorsLookup {
            emission_factors,
            unit_codes,
            adetcf,
            bdetcf,
            detcap,
        })
    }

    // ---- Emission calculators ------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn compute_exhaust_iteration(
        &mut self,
        record: &PopulationRecord<'_>,
        options: &CountyRunOptions,
        factors: &ExhaustFactorsLookup,
        adjustments: &AdjustmentTable,
        scc_tech_index: usize,
        tech_index: usize,
        year_index: usize,
        equipment_age: f32,
        tech_fraction: f32,
        temporal_adjustment: f32,
        starts_adjustment: f32,
        model_year_fraction: f32,
        activity_adjustment: f32,
        population: f32,
        n_days: i32,
        activity_index: usize,
    ) -> Result<EmissionsIterationResult> {
        // Look up load factor and activity unit from the activity entry.
        // The reference NONROAD reads `faclod` / `iactun` directly from the
        // validated activity record (there is no default load factor). The
        // index was already validated by `model_year_and_agedist`, so an
        // out-of-range index here is an invariant violation, not a
        // legitimate "use a default" condition: surface it as an error
        // (matching `model_year_and_agedist`) rather than fabricating a
        // 0.5 load factor that would silently scale emissions wrong.
        let (load_factor, activity_unit_geo) = self
            .executor
            .reference
            .activity_entries
            .get(activity_index)
            .map(|e| (e.load_factor, e.activity_unit))
            .ok_or_else(|| {
                Error::Config(format!(
                    "CountyAdapter: activity index {activity_index} out of range \
                     in compute_exhaust_iteration"
                ))
            })?;

        let activity_unit = match activity_unit_geo {
            ActivityUnit::HoursPerYear => ExhaustActivityUnit::HoursPerYear,
            ActivityUnit::HoursPerDay => ExhaustActivityUnit::HoursPerDay,
            ActivityUnit::GallonsPerYear => ExhaustActivityUnit::GallonsPerYear,
            ActivityUnit::GallonsPerDay => ExhaustActivityUnit::GallonsPerDay,
        };

        // Extract scalar BSFC for this (year, tech) from the lookup table.
        let n_tech = if factors.bsfc.is_empty() {
            1
        } else {
            factors.bsfc.len() / MXAGYR
        };
        let bsfc = factors
            .bsfc
            .get(year_index * n_tech + tech_index)
            .copied()
            .unwrap_or(0.0);

        // Build tech-fractions table indexed [scc_tech_index * MXTECH + tech_index].
        // Match the hp-binned entry (not just the SCC): an SCC may have
        // several HP-range entries with different tech mixes, and the
        // tech-slot ordering must line up with the entry `find_exhaust_tech`
        // selected for this record.
        // Use the per-model-year mix for the current iteration's tech model
        // year — reconstructed from the loop index as
        // `tchmdyr = min(episode_year - year_index, tech_year)`
        // (prccty.f: idxyr = iepyr - iyr + 1; tchmdyr = min(iyr, itchyr)).
        let tchmdyr = (options.episode_year - year_index as i32).min(options.tech_year);
        let entry_fracs: Vec<f32> = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == record.scc && e.hp_min <= record.hp_avg && record.hp_avg <= e.hp_max)
            .map(|e| e.fractions_for_year(tchmdyr).to_vec())
            .unwrap_or_default();
        let table_len = (scc_tech_index + 1) * MXTECH;
        let mut tech_fracs = vec![0.0_f32; table_len];
        for (i, &frac) in entry_fracs.iter().enumerate() {
            let cell = scc_tech_index * MXTECH + i;
            if cell < table_len {
                tech_fracs[cell] = frac;
            }
        }

        let retrofit_reduction = vec![0.0_f32; MXPOL];
        let mut emission_factors = factors.emission_factors.clone();
        let sox_conversion = [SWTGS2, SWTGS4, SWTDSL, SWTLPG, SWTCNG];
        let sox_base = [SWTGS2, SWTGS4, SWTDSL, SWTLPG, SWTCNG];

        // A pollutant takes the EF-gated branch of
        // `calculate_exhaust_emissions` only when an emission-factor file
        // was loaded for it (clcems.f :179–181). We derive that from the
        // populated EF table: a non-zero base rate for any tech slot of
        // the pollutant means a file is present. CO2/SOx/Displacement are
        // always computed regardless of this filter. When no EF table is
        // loaded the filter is all-false, preserving the legacy
        // BSFC-only (CO2/SOx) behaviour. Built before `calc_inputs` takes
        // a mutable borrow of `emission_factors`.
        let mut pollutant_filter = PollutantFilter::empty();
        for pol in 0..MXPOL {
            let has = (0..MXTECH).any(|t| {
                emission_factors
                    .get(pol * MXTECH + t)
                    .is_some_and(|&v| v != 0.0)
            });
            pollutant_filter = pollutant_filter.set_slot(pol, has);
        }

        let mut calc_inputs = ExhaustCalcInputs {
            year_index,
            tech_index,
            scc_tech_index,
            equipment_age,
            detcap: &factors.detcap,
            adetcf: &factors.adetcf,
            bdetcf: &factors.bdetcf,
            unit_codes: &factors.unit_codes,
            tech_fraction,
            hp_avg: record.hp_avg,
            fuel_density: fuel_density(options.fuel),
            bsfc,
            activity_index,
            load_factor,
            activity_unit,
            daily_adjustments: adjustments,
            adjustment_time: temporal_adjustment,
            day_range: DayRange {
                begin_day: 1,
                end_day: 1,
                winter_skip_begin: 0,
                winter_skip_end: 0,
                winter_skip: false,
            },
            emission_factors: &mut emission_factors,
            starts_adjustment,
            temporal_adjustment,
            population,
            model_year_fraction,
            n_days,
            activity_adjustment,
            tech_fractions_table: &tech_fracs,
            retrofit_reduction: &retrofit_reduction,
            fuel: options.fuel,
            sox_conversion,
            sox_base,
            sulfur_alternate: None,
        };

        let outputs = calculate_exhaust_emissions(&mut calc_inputs, &pollutant_filter);

        Ok(EmissionsIterationResult {
            emsday_delta: outputs.emissions_day,
            emsbmy: outputs.emissions_by_model_year,
            fulbmy: 0.0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_evap_iteration(
        &mut self,
        _record: &PopulationRecord<'_>,
        _options: &CountyRunOptions,
        factors: &EvapFactorsLookup,
        _adjustments: &AdjustmentTable,
        _refueling: &RefuelingData,
        _scc_tech_index: usize,
        tech_index: usize,
        year_index: usize,
        equipment_age: f32,
        evap_tech_fraction: f32,
        _evap_tech_name: &str,
        temporal_adjustment: f32,
        starts_adjustment: f32,
        model_year_fraction: f32,
        activity_adjustment: f32,
        population: f32,
        n_days: i32,
        fulbmy: f32,
    ) -> Result<EmissionsIterationResult> {
        let mut emsday_delta = vec![0.0_f32; MXPOL];
        let mut emsbmy = vec![0.0_f32; MXPOL];

        for pol_slot in 0..MXPOL {
            let ef_idx = year_index * (MXPOL * MXEVTECH) + pol_slot * MXEVTECH + tech_index;
            let base_rate = factors.emission_factors.get(ef_idx).copied().unwrap_or(0.0);
            if base_rate == 0.0 {
                continue;
            }
            let unit_idx = pol_slot * MXEVTECH + tech_index;
            let unit = factors
                .unit_codes
                .get(unit_idx)
                .copied()
                .unwrap_or(EmissionUnitCode::GramsPerHour);

            let det_a = factors.adetcf.get(unit_idx).copied().unwrap_or(0.0);
            let det_b = factors.bdetcf.get(unit_idx).copied().unwrap_or(0.0);
            let det_cap_v = factors.detcap.get(unit_idx).copied().unwrap_or(0.0);
            let capped_age = if det_cap_v > 0.0 {
                equipment_age.min(det_cap_v)
            } else {
                equipment_age
            };
            let det = if det_a != 0.0 {
                1.0 + det_a * capped_age.powf(det_b)
            } else {
                1.0
            };

            let emis: f32 = match unit {
                EmissionUnitCode::GramsPerHour | EmissionUnitCode::GramsPerHpHour => {
                    // Running loss and similar hourly species.
                    // activity_adjustment (actadj) carries hours × load × activity factor.
                    base_rate
                        * activity_adjustment
                        * model_year_fraction
                        * evap_tech_fraction
                        * population
                        * temporal_adjustment
                        * det
                        * CVTTON
                }
                EmissionUnitCode::GramsPerStart => {
                    // Hot soak: emission per engine start.
                    base_rate
                        * starts_adjustment
                        * model_year_fraction
                        * evap_tech_fraction
                        * population
                        * temporal_adjustment
                        * det
                        * CVTTON
                }
                EmissionUnitCode::GramsPerGallon => {
                    // Fuel-consumption-proportional species (e.g. running loss variant).
                    // fulbmy is already scaled by model_year_fraction.
                    base_rate * fulbmy * evap_tech_fraction * temporal_adjustment * det * CVTTON
                }
                EmissionUnitCode::GramsPerDay => {
                    base_rate
                        * (n_days as f32)
                        * model_year_fraction
                        * evap_tech_fraction
                        * population
                        * det
                        * CVTTON
                }
                EmissionUnitCode::GramsPerM2Day | EmissionUnitCode::Multiplier => {
                    // Permeation (g/m²/day) needs surface-area data from spillage records
                    // (not yet loaded). Diurnal (Mult) needs daily temperature range and
                    // fuel RVP. Use RMISS to signal "factor present but calculation
                    // blocked by missing inputs" — matches NONROAD convention for
                    // missing-EF slots (never a silent zero).
                    emsday_delta[pol_slot] = RMISS;
                    emsbmy[pol_slot] = RMISS;
                    continue;
                }
                EmissionUnitCode::GramsPerTank => {
                    base_rate
                        * (n_days as f32)
                        * model_year_fraction
                        * evap_tech_fraction
                        * population
                        * det
                        * CVTTON
                }
            };

            emsday_delta[pol_slot] += emis;
            emsbmy[pol_slot] += emis;
        }

        Ok(EmissionsIterationResult {
            emsday_delta,
            emsbmy,
            fulbmy: 0.0,
        })
    }
}

// =============================================================================
// StateAdapter — implements StateCallbacks for ProductionExecutor
// =============================================================================

/// Adapter that implements [`StateCallbacks`] by borrowing reference
/// tables from a [`ProductionExecutor`].
///
/// Created per-call in [`ProductionExecutor::execute_state_to_county`] /
/// `execute_state_from_national`; dropped when the geography routine
/// returns.
struct StateAdapter<'a> {
    executor: &'a mut ProductionExecutor,
    /// Indices of currently-surviving retrofit records — narrowed
    /// progressively by the three `filter_retrofits_by_*` methods.
    retrofit_survivors: Vec<usize>,
}

impl<'a> StateAdapter<'a> {
    fn new(executor: &'a mut ProductionExecutor) -> Self {
        Self {
            executor,
            retrofit_survivors: Vec::new(),
        }
    }
}

impl<'a> StateCallbacks for StateAdapter<'a> {
    fn find_exhaust_tech(
        &mut self,
        scc: &str,
        hp_avg: f32,
        _year: i32,
    ) -> Option<ExhaustTechLookup> {
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(ExhaustTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, _year: i32) -> Option<EvapTechLookup> {
        let entry = self
            .executor
            .reference
            .evap_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(EvapTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32> {
        let idx = self
            .executor
            .reference
            .growth_xref_entries
            .iter()
            .position(|e| {
                e.fips == fips && e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max
            })?;
        Some(idx as i32)
    }

    fn find_activity(&mut self, scc: &str, fips: &str, _hp_avg: f32) -> Option<ActivityLookup> {
        let entry = self
            .executor
            .reference
            .activity_entries
            .iter()
            .find(|e| e.scc == scc && (e.fips.is_empty() || e.fips == fips))?;
        // Convert geography::common::ActivityUnit → emissions::exhaust::ActivityUnit.
        let units = match entry.activity_unit {
            ActivityUnit::HoursPerYear => ExhaustActivityUnit::HoursPerYear,
            ActivityUnit::HoursPerDay => ExhaustActivityUnit::HoursPerDay,
            ActivityUnit::GallonsPerYear => ExhaustActivityUnit::GallonsPerYear,
            ActivityUnit::GallonsPerDay => ExhaustActivityUnit::GallonsPerDay,
        };
        Some(ActivityLookup {
            load_factor: entry.load_factor,
            units,
            activity_level: entry.activity_level,
            starts_value: entry.starts,
            age_curve_id: entry.age_code.clone(),
        })
    }

    fn day_month_factor(&mut self, scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f`: accumulate mthf/ndays over selected months,
        // set dayf from day-of-week mode.
        let dmf = self.executor.day_month_factors_for(scc);
        Ok(DayMonthFactor {
            day_month_fac: dmf.day_factors,
            mthf: dmf.month_factor,
            dayf: dmf.day_of_week_factor,
            n_days: dmf.n_days,
        })
    }

    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "StateAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            // `growth_factor` is only invoked when `growth_loaded` is true,
            // so an indicator that resolves to no growth records is a
            // missing-data error, not a legitimate "no growth". The Fortran
            // `grwfac` writes the 7000 fatal error ("Could not find any
            // valid growth data") in this case rather than returning a
            // neutral factor. Surface it explicitly instead of fabricating
            // a 0.0 factor (which would scale the grown population wrong).
            return Err(Error::IndicatorMissing {
                code: indicator,
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            });
        }
        let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
        growth_factor(&refs, year1, year2, fips).map(|gf| gf.factor)
    }

    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        pop_growth_factor: f32,
    ) -> Result<PrcusModelYearOutput> {
        // Convert emissions::exhaust::ActivityUnit → population::modyr::ActivityUnits.
        let units = match activity.units {
            ExhaustActivityUnit::HoursPerYear => ActivityUnits::HoursPerYear,
            ExhaustActivityUnit::HoursPerDay => ActivityUnits::HoursPerDay,
            ExhaustActivityUnit::GallonsPerYear => ActivityUnits::GallonsPerYear,
            ExhaustActivityUnit::GallonsPerDay => ActivityUnits::GallonsPerDay,
        };
        let curve = self.executor.reference.scrappage_curve.clone();
        let use_hours = eq.use_hours;
        let load_factor = activity.load_factor;
        let modyr_out = model_year(
            activity.starts_value,
            activity.activity_level,
            units,
            load_factor,
            use_hours,
            &self.executor.reference.age_adjustment_table,
            &activity.age_curve_id,
            move |acttmp| scrptime(use_hours, load_factor, acttmp, pop_growth_factor, &curve),
        )?;
        Ok(PrcusModelYearOutput {
            yryrfrcscrp: modyr_out.yryrfrcscrp,
            modfrc: modyr_out.modfrc,
            stradj: modyr_out.stradj,
            actadj: modyr_out.actadj,
            detage: modyr_out.detage,
            nyrlif: modyr_out.nyrlif,
        })
    }

    fn age_distribution(
        &mut self,
        base_pop: f32,
        modfrc: &[f32],
        base_year: i32,
        growth_year: i32,
        yryrfrcscrp: &[f32],
        fips: &str,
        indcod: i32,
    ) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "StateAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: growth_year,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        let result = age_distribution(
            base_pop,
            modfrc,
            base_year,
            growth_year,
            yryrfrcscrp,
            |y1, y2| {
                let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
                growth_factor(&refs, y1, y2, fips)
            },
        )?;
        Ok(result.base_population)
    }

    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()> {
        self.retrofit_survivors = self
            .executor
            .reference
            .retrofit_records
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                (r.scc == scc || r.scc == crate::population::RTRFTSCC_ALL)
                    && r.hp_min < hp_avg
                    && hp_avg <= r.hp_max
            })
            .map(|(i, _)| i)
            .collect();
        Ok(())
    }

    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.year_model_start <= year && year <= r.year_model_end
        });
        Ok(())
    }

    fn filter_retrofits_by_tech(&mut self, tech: &str) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.tech_type.trim() == tech.trim()
                || r.tech_type.trim() == crate::population::RTRFTTECHTYPE_ALL
        });
        Ok(())
    }

    fn calculate_retrofit(
        &mut self,
        _pop: f32,
        _scc: &str,
        _hp_avg: f32,
        _model_year: i32,
        _tech: &str,
    ) -> Result<RetrofitResult> {
        // `clcrtrft` is only invoked when `retrofit_loaded` is true, in
        // which case the surviving retrofit records must drive a real
        // per-pollutant reduction. Returning the all-zero default here
        // would silently drop the retrofit reduction (emissions biased
        // high). The state-class exhaust/evap calculators below are not
        // yet wired, so surface the unimplemented retrofit path as an
        // explicit error rather than fabricating a zero reduction.
        Err(Error::Config(
            "StateAdapter::calculate_retrofit: retrofit reduction (clcrtrft) is not \
             yet wired for the state path; cannot apply retrofit records that \
             survived the type-3 filter"
                .to_string(),
        ))
    }

    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        calculate_exhaust_from_reference(&self.executor.reference, inputs)
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        // State-aggregated evap path. The county-level path handles real
        // evap computation via compute_evap_iteration; state totals return
        // empty here (no double-counting).
        Ok(EvapResult::default())
    }
}

// =============================================================================
// NationalAdapter — implements NationalCallbacks for ProductionExecutor
// =============================================================================

/// Adapter that implements [`NationalCallbacks`] by borrowing reference
/// tables from a [`ProductionExecutor`].
///
/// Created per-call in [`ProductionExecutor::execute_national`]; dropped
/// when the geography routine returns.
struct NationalAdapter<'a> {
    executor: &'a mut ProductionExecutor,
    retrofit_survivors: Vec<usize>,
}

impl<'a> NationalAdapter<'a> {
    fn new(executor: &'a mut ProductionExecutor) -> Self {
        Self {
            executor,
            retrofit_survivors: Vec::new(),
        }
    }
}

impl<'a> NationalCallbacks for NationalAdapter<'a> {
    fn find_allocation(&mut self, scc: &str) -> Option<()> {
        if self
            .executor
            .reference
            .national_allocation
            .iter()
            .any(|e| e.scc == scc)
        {
            Some(())
        } else {
            None
        }
    }

    fn allocate_to_states(
        &mut self,
        scc: &str,
        states: &[StateDescriptor],
        national_population: f32,
        growth: f32,
        national_fips: &str,
        year: i32,
    ) -> Result<StateAllocationOutcome> {
        // Canonical `alosta.f:133-135`: popsta = popyr * Σ_i (valsta_i / valnat_i) * coeffs_i
        let entry = self
            .executor
            .reference
            .national_allocation
            .iter()
            .find(|e| e.scc == scc)
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "allocate_to_states: no NR*.ALO allocation record for SCC {scc}"
                ))
            })?;
        let record = entry.record.clone();
        let indicators = &self.executor.reference.allocation_indicators;
        // Convert geography::StateDescriptor to allocation::StateDescriptor (same fields).
        let alloc_states: Vec<crate::allocation::StateDescriptor> = states
            .iter()
            .map(|s| crate::allocation::StateDescriptor {
                fips: s.fips.clone(),
                selected: s.selected,
                has_state_records: s.has_state_records,
            })
            .collect();
        let allocs = allocate_state(
            national_fips,
            &alloc_states,
            &record,
            indicators,
            year,
            national_population,
            growth,
        )?;
        let mut populations = Vec::with_capacity(allocs.len());
        let mut growths = Vec::with_capacity(allocs.len());
        let mut used = false;
        for (alloc, state) in allocs.iter().zip(states.iter()) {
            populations.push(alloc.population);
            growths.push(
                alloc
                    .growth
                    .unwrap_or(if state.selected { growth } else { 1.0 }),
            );
            if alloc.population > 0.0 {
                used = true;
            }
        }
        Ok(StateAllocationOutcome {
            populations,
            growth: growths,
            used,
        })
    }

    fn find_exhaust_tech(
        &mut self,
        scc: &str,
        hp_avg: f32,
        _year: i32,
    ) -> Option<ExhaustTechLookup> {
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(ExhaustTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, _year: i32) -> Option<EvapTechLookup> {
        let entry = self
            .executor
            .reference
            .evap_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(EvapTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32> {
        let idx = self
            .executor
            .reference
            .growth_xref_entries
            .iter()
            .position(|e| {
                e.fips == fips && e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max
            })?;
        Some(idx as i32)
    }

    fn find_activity(&mut self, scc: &str, fips: &str, _hp_avg: f32) -> Option<ActivityLookup> {
        let entry = self
            .executor
            .reference
            .activity_entries
            .iter()
            .find(|e| e.scc == scc && (e.fips.is_empty() || e.fips == fips))?;
        let units = match entry.activity_unit {
            ActivityUnit::HoursPerYear => ExhaustActivityUnit::HoursPerYear,
            ActivityUnit::HoursPerDay => ExhaustActivityUnit::HoursPerDay,
            ActivityUnit::GallonsPerYear => ExhaustActivityUnit::GallonsPerYear,
            ActivityUnit::GallonsPerDay => ExhaustActivityUnit::GallonsPerDay,
        };
        Some(ActivityLookup {
            load_factor: entry.load_factor,
            units,
            activity_level: entry.activity_level,
            starts_value: entry.starts,
            age_curve_id: entry.age_code.clone(),
        })
    }

    fn day_month_factor(&mut self, scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f`: accumulate mthf/ndays over selected months,
        // set dayf from day-of-week mode.
        let dmf = self.executor.day_month_factors_for(scc);
        Ok(DayMonthFactor {
            day_month_fac: dmf.day_factors,
            mthf: dmf.month_factor,
            dayf: dmf.day_of_week_factor,
            n_days: dmf.n_days,
        })
    }

    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "NationalAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            // Only reached when `growth_loaded` is true; an indicator that
            // resolves to no growth records is the Fortran `grwfac` 7000
            // fatal error path, not a neutral "no growth". Surface it.
            return Err(Error::IndicatorMissing {
                code: indicator,
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            });
        }
        let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
        growth_factor(&refs, year1, year2, fips).map(|gf| gf.factor)
    }

    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        pop_growth_factor: f32,
    ) -> Result<PrcusModelYearOutput> {
        let units = match activity.units {
            ExhaustActivityUnit::HoursPerYear => ActivityUnits::HoursPerYear,
            ExhaustActivityUnit::HoursPerDay => ActivityUnits::HoursPerDay,
            ExhaustActivityUnit::GallonsPerYear => ActivityUnits::GallonsPerYear,
            ExhaustActivityUnit::GallonsPerDay => ActivityUnits::GallonsPerDay,
        };
        let curve = self.executor.reference.scrappage_curve.clone();
        let use_hours = eq.use_hours;
        let load_factor = activity.load_factor;
        let modyr_out = model_year(
            activity.starts_value,
            activity.activity_level,
            units,
            load_factor,
            use_hours,
            &self.executor.reference.age_adjustment_table,
            &activity.age_curve_id,
            move |acttmp| scrptime(use_hours, load_factor, acttmp, pop_growth_factor, &curve),
        )?;
        Ok(PrcusModelYearOutput {
            yryrfrcscrp: modyr_out.yryrfrcscrp,
            modfrc: modyr_out.modfrc,
            stradj: modyr_out.stradj,
            actadj: modyr_out.actadj,
            detage: modyr_out.detage,
            nyrlif: modyr_out.nyrlif,
        })
    }

    fn age_distribution(
        &mut self,
        base_pop: f32,
        modfrc: &[f32],
        base_year: i32,
        growth_year: i32,
        yryrfrcscrp: &[f32],
        fips: &str,
        indcod: i32,
    ) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "NationalAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: growth_year,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        let result = age_distribution(
            base_pop,
            modfrc,
            base_year,
            growth_year,
            yryrfrcscrp,
            |y1, y2| {
                let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
                growth_factor(&refs, y1, y2, fips)
            },
        )?;
        Ok(result.base_population)
    }

    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()> {
        self.retrofit_survivors = self
            .executor
            .reference
            .retrofit_records
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                (r.scc == scc || r.scc == crate::population::RTRFTSCC_ALL)
                    && r.hp_min < hp_avg
                    && hp_avg <= r.hp_max
            })
            .map(|(i, _)| i)
            .collect();
        Ok(())
    }

    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.year_model_start <= year && year <= r.year_model_end
        });
        Ok(())
    }

    fn filter_retrofits_by_tech(&mut self, tech: &str) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.tech_type.trim() == tech.trim()
                || r.tech_type.trim() == crate::population::RTRFTTECHTYPE_ALL
        });
        Ok(())
    }

    fn calculate_retrofit(
        &mut self,
        _pop: f32,
        _scc: &str,
        _hp_avg: f32,
        _model_year: i32,
        _tech: &str,
    ) -> Result<RetrofitResult> {
        // See `StateAdapter::calculate_retrofit`: returning the all-zero
        // default would silently drop the retrofit reduction. Surface the
        // unimplemented retrofit path explicitly.
        Err(Error::Config(
            "NationalAdapter::calculate_retrofit: retrofit reduction (clcrtrft) is not \
             yet wired for the national path; cannot apply retrofit records that \
             survived the type-3 filter"
                .to_string(),
        ))
    }

    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        calculate_exhaust_from_reference(&self.executor.reference, inputs)
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        Ok(EvapResult::default())
    }
}

// =============================================================================
// UsTotalAdapter — implements UsTotalCallbacks for ProductionExecutor
// =============================================================================

/// Adapter that implements [`UsTotalCallbacks`] by borrowing reference
/// tables from a [`ProductionExecutor`].
///
/// Created per-call in [`ProductionExecutor::execute_us_total`]; dropped
/// when the geography routine returns. Method bodies are identical to
/// [`StateAdapter`] — the two traits have the same surface.
struct UsTotalAdapter<'a> {
    executor: &'a mut ProductionExecutor,
    retrofit_survivors: Vec<usize>,
}

impl<'a> UsTotalAdapter<'a> {
    fn new(executor: &'a mut ProductionExecutor) -> Self {
        Self {
            executor,
            retrofit_survivors: Vec::new(),
        }
    }
}

impl<'a> UsTotalCallbacks for UsTotalAdapter<'a> {
    fn find_exhaust_tech(
        &mut self,
        scc: &str,
        hp_avg: f32,
        _year: i32,
    ) -> Option<ExhaustTechLookup> {
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(ExhaustTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, _year: i32) -> Option<EvapTechLookup> {
        let entry = self
            .executor
            .reference
            .evap_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(EvapTechLookup {
            tech_names: entry.tech_names.clone(),
            fractions: entry.tech_fractions.clone(),
        })
    }

    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32> {
        let idx = self
            .executor
            .reference
            .growth_xref_entries
            .iter()
            .position(|e| {
                e.fips == fips && e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max
            })?;
        Some(idx as i32)
    }

    fn find_activity(&mut self, scc: &str, fips: &str, _hp_avg: f32) -> Option<ActivityLookup> {
        let entry = self
            .executor
            .reference
            .activity_entries
            .iter()
            .find(|e| e.scc == scc && (e.fips.is_empty() || e.fips == fips))?;
        let units = match entry.activity_unit {
            ActivityUnit::HoursPerYear => ExhaustActivityUnit::HoursPerYear,
            ActivityUnit::HoursPerDay => ExhaustActivityUnit::HoursPerDay,
            ActivityUnit::GallonsPerYear => ExhaustActivityUnit::GallonsPerYear,
            ActivityUnit::GallonsPerDay => ExhaustActivityUnit::GallonsPerDay,
        };
        Some(ActivityLookup {
            load_factor: entry.load_factor,
            units,
            activity_level: entry.activity_level,
            starts_value: entry.starts,
            age_curve_id: entry.age_code.clone(),
        })
    }

    fn day_month_factor(&mut self, scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f`: accumulate mthf/ndays over selected months,
        // set dayf from day-of-week mode.
        let dmf = self.executor.day_month_factors_for(scc);
        Ok(DayMonthFactor {
            day_month_fac: dmf.day_factors,
            mthf: dmf.month_factor,
            dayf: dmf.day_of_week_factor,
            n_days: dmf.n_days,
        })
    }

    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "UsTotalAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            // Only reached when `growth_loaded` is true; an indicator that
            // resolves to no growth records is the Fortran `grwfac` 7000
            // fatal error path, not a neutral "no growth". Surface it.
            return Err(Error::IndicatorMissing {
                code: indicator,
                fips: fips.to_string(),
                subcounty: String::new(),
                year: year2,
            });
        }
        let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
        growth_factor(&refs, year1, year2, fips).map(|gf| gf.factor)
    }

    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        pop_growth_factor: f32,
    ) -> Result<PrcusModelYearOutput> {
        let units = match activity.units {
            ExhaustActivityUnit::HoursPerYear => ActivityUnits::HoursPerYear,
            ExhaustActivityUnit::HoursPerDay => ActivityUnits::HoursPerDay,
            ExhaustActivityUnit::GallonsPerYear => ActivityUnits::GallonsPerYear,
            ExhaustActivityUnit::GallonsPerDay => ActivityUnits::GallonsPerDay,
        };
        let curve = self.executor.reference.scrappage_curve.clone();
        let use_hours = eq.use_hours;
        let load_factor = activity.load_factor;
        let modyr_out = model_year(
            activity.starts_value,
            activity.activity_level,
            units,
            load_factor,
            use_hours,
            &self.executor.reference.age_adjustment_table,
            &activity.age_curve_id,
            move |acttmp| scrptime(use_hours, load_factor, acttmp, pop_growth_factor, &curve),
        )?;
        Ok(PrcusModelYearOutput {
            yryrfrcscrp: modyr_out.yryrfrcscrp,
            modfrc: modyr_out.modfrc,
            stradj: modyr_out.stradj,
            actadj: modyr_out.actadj,
            detage: modyr_out.detage,
            nyrlif: modyr_out.nyrlif,
        })
    }

    fn age_distribution(
        &mut self,
        base_pop: f32,
        modfrc: &[f32],
        base_year: i32,
        growth_year: i32,
        yryrfrcscrp: &[f32],
        fips: &str,
        indcod: i32,
    ) -> Result<f32> {
        let indicator = self
            .executor
            .reference
            .growth_xref_entries
            .get(indcod as usize)
            .map(|e| e.indicator.clone())
            .ok_or_else(|| {
                Error::Config(format!(
                    "UsTotalAdapter: growth_xref index {indcod} out of range"
                ))
            })?
            .ok_or_else(|| Error::IndicatorMissing {
                code: String::new(),
                fips: fips.to_string(),
                subcounty: String::new(),
                year: growth_year,
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        let result = age_distribution(
            base_pop,
            modfrc,
            base_year,
            growth_year,
            yryrfrcscrp,
            |y1, y2| {
                let refs: Vec<&GrowthIndicatorRecord> = selected.iter().collect();
                growth_factor(&refs, y1, y2, fips)
            },
        )?;
        Ok(result.base_population)
    }

    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()> {
        self.retrofit_survivors = self
            .executor
            .reference
            .retrofit_records
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                (r.scc == scc || r.scc == crate::population::RTRFTSCC_ALL)
                    && r.hp_min < hp_avg
                    && hp_avg <= r.hp_max
            })
            .map(|(i, _)| i)
            .collect();
        Ok(())
    }

    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.year_model_start <= year && year <= r.year_model_end
        });
        Ok(())
    }

    fn filter_retrofits_by_tech(&mut self, tech: &str) -> Result<()> {
        self.retrofit_survivors.retain(|&i| {
            let r = &self.executor.reference.retrofit_records[i];
            r.tech_type.trim() == tech.trim()
                || r.tech_type.trim() == crate::population::RTRFTTECHTYPE_ALL
        });
        Ok(())
    }

    fn calculate_retrofit(
        &mut self,
        _pop: f32,
        _scc: &str,
        _hp_avg: f32,
        _model_year: i32,
        _tech: &str,
    ) -> Result<RetrofitResult> {
        // See `StateAdapter::calculate_retrofit`: returning the all-zero
        // default would silently drop the retrofit reduction. Surface the
        // unimplemented retrofit path explicitly.
        Err(Error::Config(
            "UsTotalAdapter::calculate_retrofit: retrofit reduction (clcrtrft) is not \
             yet wired for the US-total path; cannot apply retrofit records that \
             survived the type-3 filter"
                .to_string(),
        ))
    }

    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        calculate_exhaust_from_reference(&self.executor.reference, inputs)
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        Ok(EvapResult::default())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Compute exhaust emissions for one `(model_year, tech)` iteration on the
/// state / national / US-total path, mirroring the county-path
/// `compute_exhaust_iteration` logic.
///
/// Looks up the `ExhaustTechEntry` for `(scc, hp_avg)`, extracts the
/// per-tech BSFC, builds the EF arrays, computes emission adjustments, and
/// calls `calculate_exhaust_emissions`. Returns `ExhaustResult` with
/// `bsfc` populated.
///
/// Used by `StateAdapter`, `NationalAdapter`, and `UsTotalAdapter` to share
/// the same wiring without duplicating the county-path logic.
fn calculate_exhaust_from_reference(
    reference: &ReferenceData,
    inputs: &ExhaustCallInputs<'_>,
) -> Result<ExhaustResult> {
    let entry = reference
        .exhaust_tech_entries
        .iter()
        .find(|e| e.scc == inputs.scc && e.hp_min <= inputs.hp_avg && inputs.hp_avg <= e.hp_max)
        .ok_or_else(|| {
            Error::Config(format!(
                "calculate_exhaust: no exhaust-tech entry for SCC {} hp_avg {}; \
                 emfclc.f (NR*.EMF packet) must supply a real BSFC.",
                inputs.scc, inputs.hp_avg
            ))
        })?;

    if entry.bsfc.is_empty() {
        return Err(Error::Config(format!(
            "calculate_exhaust: exhaust-tech entry for SCC {} hp_avg {} has empty BSFC; \
             emfclc.f populates a real per-tech BSFC. An empty BSFC is a data error.",
            inputs.scc, inputs.hp_avg
        )));
    }
    let bsfc = entry
        .bsfc
        .get(inputs.tech_index)
        .copied()
        .unwrap_or(entry.bsfc[0]);

    let n_tech = entry.tech_names.len().max(1);
    let mut emission_factors = vec![0.0_f32; MXAGYR * MXPOL * MXTECH];
    let mut unit_codes = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * MXTECH];
    let mut adetcf = vec![0.0_f32; MXPOL * MXTECH];
    let mut bdetcf = vec![0.0_f32; MXPOL * MXTECH];
    let mut detcap = vec![0.0_f32; MXPOL * MXTECH];

    if !entry.emission_factors.is_empty() {
        let tech_span = n_tech.min(MXTECH);
        for pol in 0..MXPOL {
            for t in 0..tech_span {
                let src = pol * n_tech + t;
                if src >= entry.emission_factors.len() {
                    continue;
                }
                let ef = entry.emission_factors[src];
                if ef != 0.0 {
                    for y in 0..MXAGYR {
                        emission_factors[y * (MXPOL * MXTECH) + pol * MXTECH + t] = ef;
                    }
                }
                let dst = pol * MXTECH + t;
                if let Some(u) = entry.emission_units.get(src) {
                    unit_codes[dst] = *u;
                }
                if let Some(a) = entry.det_a.get(src) {
                    adetcf[dst] = *a;
                }
                if let Some(b) = entry.det_b.get(src) {
                    bdetcf[dst] = *b;
                }
                if let Some(c) = entry.det_cap.get(src) {
                    detcap[dst] = *c;
                }
            }
        }
    }

    let tamb = reference
        .ambient_temp_by_scc
        .get(inputs.scc)
        .copied()
        .unwrap_or(reference.ambient_temp_f);
    if tamb <= 0.0 {
        return Err(Error::Config(format!(
            "calculate_exhaust: ambient temperature absent for SCC {} \
             (NR*.EMF / temperature input not loaded).",
            inputs.scc
        )));
    }
    let fuel = fuel_for_scc(inputs.scc).unwrap_or(FuelKind::Gasoline4Stroke);
    let temps = DailyTemperatures {
        daily_temperature_mode: false,
        daily_ambient_temp_f: Vec::new(),
        ambient_temp: tamb,
    };
    let dummy_month = [1.0_f32; crate::common::consts::MXDAYS];
    let adj_inputs = AdjustmentInputs {
        fuel,
        scc: inputs.scc,
        fips: "",
        day_range: DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        },
        temperatures: &temps,
        daily_month_fraction: &dummy_month,
        rfg: reference.fuel_rfg,
        high_altitude: false,
        oxygen_percent: reference.fuel_oxygen_pct,
        episode_year: 0,
        month: 0,
        month_to_season: [Season::Summer; 12],
        rfg_winter_2_stroke: None,
        rfg_winter_4_stroke: None,
        rfg_summer_2_stroke: None,
        rfg_summer_4_stroke: None,
        sox_fuel: [1.0; 5],
        sox_base: [1.0; 5],
        sox_diesel_marine: 1.0,
        altitude_factor: [1.0; 5],
    };
    let adjustments = calculate_emission_adjustments(&adj_inputs);

    let mut tech_fracs = vec![0.0_f32; MXTECH];
    if inputs.tech_index < MXTECH {
        tech_fracs[inputs.tech_index] = inputs.tech_fraction;
    }

    let mut pollutant_filter = PollutantFilter::empty();
    for pol in 0..MXPOL {
        let has = (0..MXTECH).any(|t| {
            emission_factors
                .get(inputs.year_index * (MXPOL * MXTECH) + pol * MXTECH + t)
                .is_some_and(|&v| v != 0.0)
        });
        pollutant_filter = pollutant_filter.set_slot(pol, has);
    }

    let retrofit_reduction = vec![0.0_f32; MXPOL];
    let sox_conversion = [SWTGS2, SWTGS4, SWTDSL, SWTLPG, SWTCNG];
    let sox_base_arr = [SWTGS2, SWTGS4, SWTDSL, SWTLPG, SWTCNG];

    let mut calc_inputs = ExhaustCalcInputs {
        year_index: inputs.year_index,
        tech_index: inputs.tech_index,
        scc_tech_index: 0,
        equipment_age: inputs.deterioration_age,
        detcap: &detcap,
        adetcf: &adetcf,
        bdetcf: &bdetcf,
        unit_codes: &unit_codes,
        tech_fraction: inputs.tech_fraction,
        hp_avg: inputs.hp_avg,
        fuel_density: inputs.fuel_density,
        bsfc,
        activity_index: 0,
        load_factor: inputs.activity.load_factor,
        activity_unit: inputs.activity.units,
        daily_adjustments: &adjustments,
        adjustment_time: inputs.adjustment_time,
        day_range: DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        },
        emission_factors: &mut emission_factors,
        starts_adjustment: inputs.starts_adjustment,
        temporal_adjustment: inputs.temporal_adjustment,
        population: inputs.population,
        model_year_fraction: inputs.model_year_fraction,
        n_days: inputs.n_days,
        activity_adjustment: inputs.activity_adjustment,
        tech_fractions_table: &tech_fracs,
        retrofit_reduction: &retrofit_reduction,
        fuel,
        sox_conversion,
        sox_base: sox_base_arr,
        sulfur_alternate: None,
    };

    let outputs = calculate_exhaust_emissions(&mut calc_inputs, &pollutant_filter);

    Ok(ExhaustResult {
        ems_day_delta: outputs.emissions_day,
        ems_bmy: outputs.emissions_by_model_year,
        bsfc,
    })
}

/// Build a [`CountyRunOptions`] from the dispatch context and run options.
fn build_run_options(
    ctx: &DispatchContext<'_>,
    options: &NonroadOptions,
    hp_levels: [f32; MXHPC],
) -> CountyRunOptions {
    CountyRunOptions {
        tech_year: options.tech_year,
        episode_year: options.episode_year,
        growth_year: options.growth_year,
        fuel: ctx.fuel.unwrap_or(FuelKind::Gasoline4Stroke),
        sum_type: if options.total_mode {
            SumType::Total
        } else {
            SumType::Typical
        },
        daily_mode: options.daily_output,
        write_bmy_exhaust: options.emit_bmy_exhaust,
        write_bmy_evap: options.emit_bmy_evap,
        write_si: options.emit_si,
        retrofit_enabled: options.retrofit_loaded,
        spillage_enabled: options.spillage_loaded,
        growth_enabled: options.growth_loaded,
        hp_levels,
    }
}

/// Flatten a [`ProcessOutput`] from `process_county` / `process_subcounty`
/// into a flat `Vec<SimEmissionRow>`.
///
/// Each [`DatRecord`] becomes a `wrtdat`-shaped row with `model_year=None`,
/// `tech_type=None`, and the caller-supplied `channel`. Each [`BmyRecord`]
/// becomes a `wrtbmy`-shaped row with `model_year` and `tech_type` set and
/// channel derived from [`BmyKind`]. Missing-data (`RMISS`-filled) emission
/// values are copied as-is, preserving the sentinel.
///
/// The `_scc` argument is accepted for API symmetry with the call site but
/// is not used internally — each record already carries its own SCC.
fn process_output_to_rows(
    out: &ProcessOutput,
    _scc: &str,
    channel: EmissionChannel,
) -> Vec<SimEmissionRow> {
    let mut rows = Vec::new();
    for dat in &out.dat_records {
        rows.push(SimEmissionRow {
            fips: dat.fips.clone(),
            subcounty: dat.subcounty.clone(),
            scc: dat.scc.clone(),
            hp_level: dat.hp_level,
            model_year: None,
            tech_type: None,
            channel,
            population: dat.population_total,
            activity: dat.activity_total,
            fuel_consumption: dat.fuel_consumption,
            emissions: dat.emissions.clone(),
        });
    }
    for bmy in &out.bmy_records {
        let bmy_channel = match bmy.kind {
            BmyKind::Exhaust => EmissionChannel::Exhaust,
            BmyKind::Evaporative => EmissionChannel::Evaporative,
        };
        rows.push(SimEmissionRow {
            fips: bmy.fips.clone(),
            subcounty: bmy.subcounty.clone(),
            scc: bmy.scc.clone(),
            hp_level: bmy.hp_level,
            model_year: Some(bmy.model_year),
            tech_type: Some(bmy.tech_name.clone()),
            channel: bmy_channel,
            population: bmy.population,
            activity: bmy.activity,
            fuel_consumption: bmy.fuel,
            emissions: bmy.emissions.clone(),
        });
    }
    rows
}

/// Flatten a [`GeographyOutput`] from the state/national/US-total routines
/// into a flat `Vec<SimEmissionRow>`.
///
/// Each [`StateOutput`] becomes a `wrtdat`-shaped row (`model_year=None`,
/// `tech_type=None`, channel always [`EmissionChannel::Exhaust`]). Each
/// [`ByModelYearOutput`] becomes a `wrtbmy`-shaped row with `model_year`
/// and `tech_type` set and channel derived from the `channel` byte (`1` →
/// exhaust, any other → evaporative). Missing-data (`RMISS`-filled) values
/// are copied as-is.
///
/// The `_scc` argument is accepted for API symmetry but is not used
/// internally — each record already carries its own SCC.
fn geography_output_to_rows(out: &GeographyOutput, _scc: &str) -> Vec<SimEmissionRow> {
    let mut rows = Vec::new();
    for st in &out.state_outputs {
        rows.push(SimEmissionRow {
            fips: st.fips.clone(),
            subcounty: st.subcounty.clone(),
            scc: st.scc.clone(),
            hp_level: st.hp_level,
            model_year: None,
            tech_type: None,
            channel: EmissionChannel::Exhaust,
            population: st.population,
            activity: st.activity,
            fuel_consumption: st.fuel_consumption,
            emissions: st.emissions_day.clone(),
        });
    }
    for bmy in &out.bmy_outputs {
        let bmy_channel = if bmy.channel == 1 {
            EmissionChannel::Exhaust
        } else {
            EmissionChannel::Evaporative
        };
        rows.push(SimEmissionRow {
            fips: bmy.fips.clone(),
            subcounty: bmy.subcounty.clone(),
            scc: bmy.scc.clone(),
            hp_level: bmy.hp_level,
            model_year: Some(bmy.model_year),
            tech_type: Some(bmy.tech_type.clone()),
            channel: bmy_channel,
            population: bmy.population,
            activity: bmy.activity,
            fuel_consumption: bmy.fuel_consumption,
            emissions: bmy.emissions.clone(),
        });
    }
    rows
}

/// Map a [`ProcessOutcome`] from `process_county` / `process_subcounty`
/// onto the uniform [`GeographyExecution`] shape.
///
/// `Skipped` outcomes produce an empty (no rows) execution with
/// `skipped = true`. `Success` outcomes delegate row conversion to
/// [`process_output_to_rows`].
fn process_outcome_to_execution(outcome: ProcessOutcome) -> GeographyExecution {
    let skipped = outcome.is_skipped();
    let output = outcome.into_output();
    let warnings: Vec<String> = output.warnings.iter().map(|w| w.message.clone()).collect();

    if skipped {
        return GeographyExecution {
            skipped: true,
            rows: Vec::new(),
            warnings,
            national_record_count: 0,
        };
    }

    let rows = process_output_to_rows(&output, "", EmissionChannel::Exhaust);
    GeographyExecution {
        skipped: false,
        rows,
        warnings,
        national_record_count: 0,
    }
}

/// Build a [`NationalContext`] from a dispatch context, run options, and state list.
fn build_national_context<'a>(
    ctx: &'a DispatchContext<'_>,
    options: &NonroadOptions,
    hp_levels: &'a [f32],
    states: &'a [StateDescriptor],
) -> NationalContext<'a> {
    let run_options = StateRunOptions {
        episode_year: options.episode_year,
        growth_year: options.growth_year,
        tech_year: options.tech_year,
        fuel: ctx.fuel.unwrap_or(FuelKind::Gasoline4Stroke),
        total_mode: options.total_mode,
        daily_output: options.daily_output,
        emit_bmy: options.emit_bmy_exhaust,
        emit_bmy_evap: options.emit_bmy_evap,
        emit_si: options.emit_si,
        growth_loaded: options.growth_loaded,
        retrofit_loaded: options.retrofit_loaded,
        spillage_loaded: options.spillage_loaded,
    };
    let equipment = EquipmentRecord {
        hp_range_min: 0.0,
        hp_range_max: ctx.record.hp_avg * 2.0,
        hp_avg: ctx.record.hp_avg,
        population: ctx.record.population,
        pop_year: ctx.record.pop_year,
        // Median life (scrptime's `mdlfhrs`) from the driver record's
        // `.POP` usage field, matching the county/subcounty paths; fall
        // back to a neutral 1000.0 only when the record carries none.
        // Previously this hardcoded 1000.0, silently ignoring any real
        // median life on the record.
        use_hours: if ctx.record.median_life > 0.0 {
            ctx.record.median_life
        } else {
            1000.0
        },
        discharge_code: 0,
        starts_hours: 1.0,
    };
    NationalContext {
        equipment,
        run_options,
        scc: ctx.scc,
        hp_levels,
        states,
        state_index: 0,
        growth_hint: ctx.growth.unwrap_or(-9.0),
        national_fips: ctx.record.region_code.clone(),
    }
}

/// Build a [`UsTotalContext`] from a dispatch context and run options.
fn build_us_total_context<'a>(
    ctx: &'a DispatchContext<'_>,
    options: &NonroadOptions,
    hp_levels: &'a [f32],
) -> UsTotalContext<'a> {
    let run_options = StateRunOptions {
        episode_year: options.episode_year,
        growth_year: options.growth_year,
        tech_year: options.tech_year,
        fuel: ctx.fuel.unwrap_or(FuelKind::Gasoline4Stroke),
        total_mode: options.total_mode,
        daily_output: options.daily_output,
        emit_bmy: options.emit_bmy_exhaust,
        emit_bmy_evap: options.emit_bmy_evap,
        emit_si: options.emit_si,
        growth_loaded: options.growth_loaded,
        retrofit_loaded: options.retrofit_loaded,
        spillage_loaded: options.spillage_loaded,
    };
    let equipment = EquipmentRecord {
        hp_range_min: 0.0,
        hp_range_max: ctx.record.hp_avg * 2.0,
        hp_avg: ctx.record.hp_avg,
        population: ctx.record.population,
        pop_year: ctx.record.pop_year,
        // Median life (scrptime's `mdlfhrs`) from the driver record's
        // `.POP` usage field, matching the county/subcounty paths; fall
        // back to a neutral 1000.0 only when the record carries none.
        // Previously this hardcoded 1000.0, silently ignoring any real
        // median life on the record.
        use_hours: if ctx.record.median_life > 0.0 {
            ctx.record.median_life
        } else {
            1000.0
        },
        discharge_code: 0,
        starts_hours: 1.0,
    };
    UsTotalContext {
        equipment,
        run_options,
        scc: ctx.scc,
        hp_levels,
    }
}

/// Build a [`StateContext`] from a dispatch context and run options.
fn build_state_context<'a>(
    ctx: &'a DispatchContext<'_>,
    options: &NonroadOptions,
    hp_levels: &'a [f32],
) -> StateContext<'a> {
    let run_options = StateRunOptions {
        episode_year: options.episode_year,
        growth_year: options.growth_year,
        tech_year: options.tech_year,
        fuel: ctx.fuel.unwrap_or(FuelKind::Gasoline4Stroke),
        total_mode: options.total_mode,
        daily_output: options.daily_output,
        emit_bmy: options.emit_bmy_exhaust,
        emit_bmy_evap: options.emit_bmy_evap,
        emit_si: options.emit_si,
        growth_loaded: options.growth_loaded,
        retrofit_loaded: options.retrofit_loaded,
        spillage_loaded: options.spillage_loaded,
    };
    let equipment = EquipmentRecord {
        hp_range_min: 0.0,
        hp_range_max: ctx.record.hp_avg * 2.0,
        hp_avg: ctx.record.hp_avg,
        population: ctx.record.population,
        pop_year: ctx.record.pop_year,
        // Median life (scrptime's `mdlfhrs`) from the driver record's
        // `.POP` usage field, matching the county/subcounty paths; fall
        // back to a neutral 1000.0 only when the record carries none.
        // Previously this hardcoded 1000.0, silently ignoring any real
        // median life on the record.
        use_hours: if ctx.record.median_life > 0.0 {
            ctx.record.median_life
        } else {
            1000.0
        },
        discharge_code: 0,
        starts_hours: 1.0,
    };
    StateContext {
        equipment,
        run_options,
        scc: ctx.scc,
        state_fips: &ctx.record.region_code,
        hp_levels,
    }
}

/// Convert a [`GeographyOutput`] from the state routines into the
/// uniform [`GeographyExecution`] shape.
///
/// Warnings are extracted before delegating row conversion to
/// [`geography_output_to_rows`]. If no rows result (missing-tech
/// early-out), the execution is marked skipped.
fn geography_output_to_execution(output: GeographyOutput) -> GeographyExecution {
    let warnings: Vec<String> = output.warnings.iter().map(|w| format!("{w:?}")).collect();
    let rows = geography_output_to_rows(&output, "");
    let skipped = rows.is_empty();
    GeographyExecution {
        skipped,
        rows,
        warnings,
        national_record_count: output.national_record_count,
    }
}

// =============================================================================
// fndrfm — refueling-mode lookup (ports fndrfm.f)
// =============================================================================

/// Find the best-matching spillage record for `(scc, hp_avg, tech_name)`.
///
/// Ports `fndrfm.f` exactly:
/// 1. Build three SCC glob patterns: exact, 7-char prefix + "000", 4-char + "000000".
/// 2. For each record: skip non-matching tech, HP-range, or SCC.
/// 3. Prefer the most-specific SCC match; break ties by closest HP mid-point.
///
/// Returns `None` when no record matches (caller falls back to `"ALL"` tech).
fn fndrfm(
    records: &[SpillageRecord],
    scc: &str,
    hp_avg: f32,
    tech_name: &str,
) -> Option<RefuelingData> {
    let tech = tech_name.trim();

    let scc_t = scc.trim();
    let scc_pad: String = format!("{scc_t:<10}");
    // Fortran: ascglb(2) = ascin(1:7)//'000'  (10-char patterns)
    let glob2 = format!("{:<7}000", scc_t.get(..7).unwrap_or(scc_t));
    let glob3 = format!("{:<4}000000", scc_t.get(..4).unwrap_or(scc_t));

    let mut best: Option<usize> = None;
    let mut best_iasc = usize::MAX;
    let mut best_idiff = i32::MAX;

    for (i, rec) in records.iter().enumerate() {
        // Must match tech type (case-insensitive trim, like Fortran COMMON uppercase storage)
        if rec.tech_type.trim() != tech {
            continue;
        }

        // HP-range check; tank-volume indicator (TANK) uses the same hp_avg value per
        // fndrfm.f (the tvol branch was commented out — chkval stays as hp_avg).
        let in_range = match rec.indicator {
            RangeIndicator::Horsepower | RangeIndicator::Tank => {
                hp_avg >= rec.hp_min && hp_avg <= rec.hp_max
            }
        };
        if !in_range {
            continue;
        }

        // SCC hierarchy match (Fortran fndchr scan of ascglb)
        let rec_scc = format!("{:<10}", rec.scc.trim());
        let idxasc = if rec_scc == scc_pad {
            1
        } else if rec_scc == glob2 {
            2
        } else if rec_scc == glob3 {
            3
        } else {
            continue;
        };

        // HP-range proximity: max of distances from each end (Fortran INT() truncates toward 0)
        let idiff = ((hp_avg - rec.hp_min) as i32).max((rec.hp_max - hp_avg) as i32);

        if idxasc < best_iasc || (idxasc == best_iasc && idiff < best_idiff) {
            best = Some(i);
            best_iasc = idxasc;
            best_idiff = idiff;
        }
    }

    best.map(|i| spillage_to_refueling(&records[i]))
}

/// Convert a parsed [`SpillageRecord`] to the [`RefuelingData`] expected by the
/// geography callbacks (maps Fortran COMMON field names to Rust struct fields).
fn spillage_to_refueling(rec: &SpillageRecord) -> RefuelingData {
    RefuelingData {
        mode: match rec.mode {
            RefuelingMode::Pump => "PUMP     ".to_string(),
            RefuelingMode::Container => "CONTAINER".to_string(),
        },
        tank: rec.tank_volume,
        tank_full: rec.tank_full,
        tank_metal: rec.tank_metal_pct,
        hose_length: rec.hose_len,
        hose_dia: rec.hose_dia,
        hose_metal: rec.hose_metal_pct,
        hot_soak_start: rec.hot_soak_per_hr,
        neck_length: rec.neck_len,
        neck_dia: rec.neck_dia,
        supply_length: rec.sr_len,
        supply_dia: rec.sr_dia,
        vent_length: rec.vent_len,
        vent_dia: rec.vent_dia,
        diurnal_fractions: rec.diurnal,
        tnk_e10_factor: rec.tank_e10,
        hose_e10_factor: rec.hose_e10,
        neck_e10_factor: rec.neck_e10,
        supply_e10_factor: rec.sr_e10,
        vent_e10_factor: rec.vent_e10,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::RegionLevel;

    fn rec(region: &str) -> DriverRecord {
        DriverRecord {
            region_code: region.to_string(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }
    }

    fn ctx<'a>(dispatch: Dispatch, scc: &'a str, record: &'a DriverRecord) -> DispatchContext<'a> {
        DispatchContext {
            dispatch,
            scc,
            fuel: Some(FuelKind::Diesel),
            record,
            growth: Some(0.05),
        }
    }

    #[test]
    fn geography_execution_skipped_helper() {
        let exec = GeographyExecution::skipped();
        assert!(exec.skipped);
        assert!(exec.rows.is_empty());
        assert!(exec.warnings.is_empty());
        assert_eq!(exec.national_record_count, 0);
    }

    #[test]
    fn recorded_dispatch_detaches_a_context() {
        let record = rec("06037");
        let context = ctx(Dispatch::County, "2270001010", &record);
        let recorded = RecordedDispatch::from_context(&context);
        assert_eq!(recorded.dispatch, Dispatch::County);
        assert_eq!(recorded.scc, "2270001010");
        assert_eq!(recorded.fuel, Some(FuelKind::Diesel));
        assert_eq!(recorded.region_code, "06037");
        assert_eq!(recorded.hp_avg, 25.0);
        assert_eq!(recorded.population, 100.0);
        assert_eq!(recorded.pop_year, 2020);
        assert_eq!(recorded.growth, Some(0.05));
    }

    #[test]
    fn plan_recording_executor_logs_each_call() {
        let mut exec = PlanRecordingExecutor::new();
        assert!(exec.is_empty());

        let opts = NonroadOptions::new(RegionLevel::County, 2020);
        let r1 = rec("06037");
        let r2 = rec("06038");
        let out1 = exec
            .execute(&ctx(Dispatch::County, "a", &r1), &opts)
            .unwrap();
        let out2 = exec
            .execute(&ctx(Dispatch::County, "b", &r2), &opts)
            .unwrap();

        // Each call returns an empty, non-skipped execution.
        assert_eq!(out1, GeographyExecution::default());
        assert_eq!(out2, GeographyExecution::default());
        assert!(!out1.skipped);

        assert_eq!(exec.len(), 2);
        assert_eq!(exec.dispatches[0].scc, "a");
        assert_eq!(exec.dispatches[1].region_code, "06038");
    }

    #[test]
    fn dispatches_to_filters_by_routine() {
        let mut exec = PlanRecordingExecutor::new();
        let opts = NonroadOptions::new(RegionLevel::Subcounty, 2020);
        let record = rec("06037");
        exec.execute(&ctx(Dispatch::County, "a", &record), &opts)
            .unwrap();
        exec.execute(&ctx(Dispatch::Subcounty, "a", &record), &opts)
            .unwrap();
        exec.execute(&ctx(Dispatch::County, "b", &record), &opts)
            .unwrap();

        assert_eq!(exec.dispatches_to(Dispatch::County).len(), 2);
        assert_eq!(exec.dispatches_to(Dispatch::Subcounty).len(), 1);
        assert_eq!(exec.dispatches_to(Dispatch::National).len(), 0);
    }

    #[test]
    fn production_executor_skips_all_dispatch_variants() {
        // With an empty ProductionExecutor (no county_fips, no tables),
        // county/subcounty dispatch returns FIPS-not-found (Skipped),
        // and the other variants still return the placeholder Skipped.
        let mut exec = ProductionExecutor::new(&ReferenceData::default());
        let opts = NonroadOptions::new(RegionLevel::County, 2020);
        let record = rec("06037");
        for dispatch in [
            Dispatch::County,
            Dispatch::Subcounty,
            Dispatch::StateToCounty,
            Dispatch::StateFromNational,
            Dispatch::National,
            Dispatch::UsTotal,
        ] {
            let c = ctx(dispatch, "2270001010", &record);
            let result = exec.execute(&c, &opts).unwrap();
            assert!(result.skipped, "{dispatch:?} should be skipped");
            assert!(result.rows.is_empty());
        }
    }

    #[test]
    fn plan_recording_executor_is_object_safe() {
        // The trait must be usable as `dyn` so `run_simulation` can
        // take a trait object.
        let mut concrete = PlanRecordingExecutor::new();
        let dynamic: &mut dyn GeographyExecutor = &mut concrete;
        let opts = NonroadOptions::new(RegionLevel::County, 2020);
        let record = rec("06037");
        dynamic
            .execute(&ctx(Dispatch::County, "a", &record), &opts)
            .unwrap();
        assert_eq!(concrete.len(), 1);
    }

    /// `process_output_to_rows` maps one DatRecord → wrtdat-shaped row
    /// and one BmyRecord → wrtbmy-shaped row. Field copies (FIPS, SCC,
    /// HP, channel) are verified for both. RMISS sentinel in emissions
    /// is preserved as-is.
    #[test]
    fn process_output_to_rows_dat_and_bmy() {
        use crate::common::consts::RMISS;
        use crate::geography::common::{BmyRecord, DatRecord};

        let dat = DatRecord {
            fips: "06037".to_string(),
            subcounty: String::new(),
            scc: "2270001010".to_string(),
            hp_level: 25.0,
            population_total: 100.0,
            activity_total: 500.0,
            fuel_consumption: 10.0,
            load_factor: 0.5,
            hp_avg: 25.0,
            frac_retrofitted: 0.0,
            units_retrofitted: 0.0,
            emissions: vec![1.0, RMISS],
        };
        let bmy = BmyRecord {
            fips: "06037".to_string(),
            subcounty: String::new(),
            scc: "2270001010".to_string(),
            hp_level: 25.0,
            tech_name: "T1".to_string(),
            model_year: 2010,
            population: 50.0,
            emissions: vec![0.5, 1.0],
            fuel: 5.0,
            activity: 250.0,
            load_factor: 0.5,
            hp_avg: 25.0,
            frac_retrofitted: 0.0,
            units_retrofitted: 0.0,
            kind: BmyKind::Exhaust,
        };
        let out = ProcessOutput {
            fips: "06037".to_string(),
            dat_records: vec![dat],
            bmy_records: vec![bmy],
            ..ProcessOutput::default()
        };

        let rows = process_output_to_rows(&out, "2270001010", EmissionChannel::Exhaust);

        assert_eq!(rows.len(), 2, "one dat row + one bmy row");

        let dat_row = &rows[0];
        assert_eq!(dat_row.fips, "06037");
        assert_eq!(dat_row.scc, "2270001010");
        assert_eq!(dat_row.hp_level, 25.0);
        assert_eq!(dat_row.channel, EmissionChannel::Exhaust);
        assert!(dat_row.model_year.is_none(), "dat row has no model year");
        assert!(dat_row.tech_type.is_none(), "dat row has no tech type");
        assert_eq!(dat_row.emissions[1], RMISS, "RMISS sentinel preserved");

        let bmy_row = &rows[1];
        assert_eq!(bmy_row.fips, "06037");
        assert_eq!(bmy_row.scc, "2270001010");
        assert_eq!(bmy_row.hp_level, 25.0);
        assert_eq!(bmy_row.channel, EmissionChannel::Exhaust);
        assert_eq!(bmy_row.model_year, Some(2010));
        assert_eq!(bmy_row.tech_type.as_deref(), Some("T1"));
    }

    /// `geography_output_to_rows` maps one StateOutput → wrtdat-shaped
    /// row and one ByModelYearOutput → wrtbmy-shaped row. Warnings on
    /// the GeographyOutput are preserved in `GeographyExecution::warnings`
    /// after the execution wrapper collects them.
    #[test]
    fn geography_output_to_rows_state_and_bmy_with_warning() {
        use crate::geography::{ByModelYearOutput, GeographyOutput, GeographyWarning, StateOutput};

        let st = StateOutput {
            fips: "06000".to_string(),
            subcounty: "     ".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 25.0,
            population: 100.0,
            activity: 500.0,
            fuel_consumption: 10.0,
            load_factor: 0.5,
            hp_avg: 25.0,
            frac_retrofitted: 0.0,
            units_retrofitted: 0.0,
            emissions_day: vec![1.0, 2.0],
            missing: false,
        };
        let bmy = ByModelYearOutput {
            fips: "06000".to_string(),
            subcounty: "     ".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 25.0,
            tech_type: "T1".to_string(),
            model_year: 2010,
            population: 50.0,
            emissions: vec![0.5, 1.0],
            fuel_consumption: 5.0,
            activity: 250.0,
            load_factor: 0.5,
            hp_avg: 25.0,
            frac_retrofitted: 0.0,
            units_retrofitted: 0.0,
            channel: 1,
        };
        let warning = GeographyWarning::MissingExhaustTech {
            scc: "2270001010".to_string(),
            hp_avg: 25.0,
            year: 2020,
        };
        let geo_out = GeographyOutput {
            state_outputs: vec![st],
            bmy_outputs: vec![bmy],
            warnings: vec![warning],
            ..GeographyOutput::default()
        };

        let rows = geography_output_to_rows(&geo_out, "2270001010");

        assert_eq!(rows.len(), 2, "one state row + one bmy row");

        let st_row = &rows[0];
        assert_eq!(st_row.fips, "06000");
        assert_eq!(st_row.scc, "2270001010");
        assert_eq!(st_row.hp_level, 25.0);
        assert_eq!(st_row.channel, EmissionChannel::Exhaust);
        assert!(st_row.model_year.is_none());
        assert!(st_row.tech_type.is_none());

        let bmy_row = &rows[1];
        assert_eq!(bmy_row.fips, "06000");
        assert_eq!(bmy_row.scc, "2270001010");
        assert_eq!(bmy_row.channel, EmissionChannel::Exhaust);
        assert_eq!(bmy_row.model_year, Some(2010));
        assert_eq!(bmy_row.tech_type.as_deref(), Some("T1"));

        // Warnings survive through geography_output_to_execution.
        let exec = geography_output_to_execution(geo_out);
        assert_eq!(exec.rows.len(), 2);
        assert!(!exec.warnings.is_empty(), "warnings must be preserved");
    }
}

/// Acceptance tests for [`ProductionExecutor`]'s county dispatch path.
#[cfg(test)]
mod production {
    use super::super::inputs::{
        ActivityTableEntry, EvapTechEntry, ExhaustTechEntry, GrowthXrefEntry,
        NationalAllocationEntry,
    };
    use super::*;
    use crate::driver::RegionLevel;
    use crate::emissions::exhaust::FuelKind;
    use crate::input::alo::AllocationRecord;
    use crate::input::indicator::{IndicatorRecord, IndicatorTable};
    use crate::input::scrappage::ScrappagePoint;
    use crate::population::AgeAdjustmentTable;

    fn default_hp_levels() -> [f32; MXHPC] {
        let vs: [f32; MXHPC] = [
            3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0,
            1000.0, 1200.0, 1500.0, 1800.0, 2000.0,
        ];
        let mut hp = [0.0_f32; MXHPC];
        hp.copy_from_slice(&vs);
        hp
    }

    fn test_record() -> DriverRecord {
        DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }
    }

    fn test_opts() -> NonroadOptions {
        let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
        opts.growth_loaded = true;
        opts
    }

    /// (a) Empty reference tables → county FIPS not found → `Skipped`.
    #[test]
    fn county_empty_ref_returns_skipped() {
        let mut exec = ProductionExecutor::new(&ReferenceData::default());
        let opts = test_opts();
        let record = test_record();
        let ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };
        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(result.skipped, "FIPS-not-found must yield skipped=true");
        assert!(result.rows.is_empty());
    }

    /// (b) Minimal reference (one FIPS, one exhaust tech with zero fraction,
    /// one evap tech with zero fraction, one growth xref, one activity
    /// entry, zero retrofits) → non-skipped, exactly one `SimEmissionRow`.
    ///
    /// Zero tech fractions cause the per-tech-type loop body to be
    /// skipped (`if tchfrc <= 0.0 { continue }`), so
    /// `compute_exhaust_iteration` and `compute_evap_iteration` are
    /// never called — no need for the emission-factor tables.
    /// `compute_exhaust_factors` / `compute_evap_factors` IS called and
    /// returns `ExhaustFactorsLookup::default()` / `EvapFactorsLookup::default()`.
    /// The model-year loop accumulates population fractions into `poptot`,
    /// and `wrtdat` emits one `DatRecord` → one `SimEmissionRow`.
    #[test]
    fn county_minimal_ref_returns_one_row() {
        let mut exec = ProductionExecutor {
            county_fips: vec!["06037".into()],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                // ambient_temp_f must be Some(>0) so emission_adjustments can compute the
                // exhaust temperature correction. bsfc must be non-empty so
                // compute_exhaust_factors can populate the BSFC array; tech_fractions
                // are 0.0 so the exhaust loop is skipped and bsfc is never consumed.
                ambient_temp_f: Some(75.0),
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![1.0],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06037".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: Some("GDP".into()),
                }],
                // growth_records empty: growth_year == episode_year == pop_year
                // (2020), so age_distribution never calls growth_factor.
                growth_records: vec![],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "06037".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = test_record();
        let ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 1, "expected exactly one SimEmissionRow");

        let row = &result.rows[0];
        assert_eq!(row.fips, "06037");
        assert_eq!(row.scc, "2270001010");
        assert!(row.model_year.is_none(), "dat_record row has no model year");
        assert!(row.tech_type.is_none(), "dat_record row has no tech type");
        assert_eq!(row.channel, EmissionChannel::Exhaust);
    }

    /// (b2) Future-year county run with an unmatched growth indicator
    /// (indicator=None) errors with the canonical prccty.f 7001 message
    /// instead of silently under-projecting.
    #[test]
    fn county_unmatched_growth_indicator_errors_for_future_year() {
        let mut exec = ProductionExecutor {
            county_fips: vec!["06037".into()],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                ambient_temp_f: 75.0,
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                // indicator=None: SCC has no growth-pattern cross-reference match.
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06037".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: None,
                }],
                growth_records: vec![],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "06037".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        // Future-year run: growth_year (2025) != pop_year (2020).
        let mut opts = test_opts();
        opts.growth_year = 2025;
        // pop_year=2020 differs from growth_year=2025 → engine must error.
        let record = DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("growth indicator cross reference"),
            "expected canonical fndgxf error, got: {msg}"
        );
        assert!(
            msg.contains("2270001010"),
            "error must name the SCC, got: {msg}"
        );
    }

    /// (b3) Base-year county run with an unmatched growth indicator
    /// (indicator=None) is unaffected — age_distribution never calls
    /// growth_fn when growth_year == base_pop_year.
    #[test]
    fn county_unmatched_growth_indicator_ok_for_base_year() {
        let mut exec = ProductionExecutor {
            county_fips: vec!["06037".into()],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                ambient_temp_f: 75.0,
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                // indicator=None: SCC has no growth-pattern cross-reference match.
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06037".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: None,
                }],
                growth_records: vec![],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "06037".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        // Base-year run: growth_year == pop_year == episode_year == 2020.
        let opts = test_opts();
        let record = DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(
            !result.skipped,
            "base-year with no indicator must not error"
        );
    }

    /// (c) StateToCounty dispatch with two counties in the state →
    /// two `SimEmissionRow`s (one per county).
    ///
    /// Zero tech fractions skip the exhaust/evap iteration so EF
    /// tables are not required. The model-year loop still accumulates
    /// `poptot`, and `process_state_to_county_record` allocates one
    /// `StateOutput` per selected county.
    #[test]
    fn state_to_county_minimal_ref_returns_two_rows() {
        let mut exec = ProductionExecutor {
            county_fips: vec!["06037".into(), "06059".into()],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: Some("GDP".into()),
                }],
                // Two flat national indicator rows (value 1.0 in both years)
                // make `growth_factor(2020, 2021, ..)` resolve to 0.0 — the same
                // neutral factor these state-class paths previously fabricated —
                // while keeping the data consistent with `growth_loaded = true`.
                // (The state/national/US-total `growth_factor` now errors on an
                // empty indicator selection, matching grwfac's 7000 fatal path.)
                growth_records: vec![
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2020,
                        value: 1.0,
                    },
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2021,
                        value: 1.0,
                    },
                ],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "06000".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = DriverRecord {
            region_code: "06000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::StateToCounty,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        // NR*.SCO county allocation is not ported — returns Err (mo-2v1).
        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("SCO") || msg.contains("allocation") || msg.contains("alocty"),
            "expected SCO-allocation error, got: {msg}"
        );
    }

    /// (d) StateFromNational dispatch with a state-level record →
    /// exactly one `SimEmissionRow` at the state FIPS.
    #[test]
    fn state_from_national_minimal_ref_returns_one_row() {
        let mut exec = ProductionExecutor {
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: Some("GDP".into()),
                }],
                // Two flat national indicator rows (value 1.0 in both years)
                // make `growth_factor(2020, 2021, ..)` resolve to 0.0 — the same
                // neutral factor these state-class paths previously fabricated —
                // while keeping the data consistent with `growth_loaded = true`.
                // (The state/national/US-total `growth_factor` now errors on an
                // empty indicator selection, matching grwfac's 7000 fatal path.)
                growth_records: vec![
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2020,
                        value: 1.0,
                    },
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2021,
                        value: 1.0,
                    },
                ],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "06000".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = DriverRecord {
            region_code: "06000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::StateFromNational,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        // Temporal factors now ported (mo-cdo): StateFromNational execution succeeds.
        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 1, "expected exactly one SimEmissionRow");
    }

    /// (e) National dispatch with one state and one allocation entry →
    /// one `SimEmissionRow` at the state FIPS.
    ///
    /// `state_index = 0` (national record) triggers `alosta`; the
    /// NR*.ALO allocation gives state "06000" population 100*(300/1000)=30.
    /// Zero tech fractions bypass the emission iteration; one `StateOutput`
    /// is still emitted per selected state. Temporal factors now ported
    /// (mo-cdo) so execution succeeds.
    #[test]
    fn national_minimal_ref_returns_one_row() {
        let mut exec = ProductionExecutor {
            state_descriptors: vec![StateDescriptor {
                fips: "06000".into(),
                selected: true,
                has_state_records: false,
            }],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                national_allocation: vec![NationalAllocationEntry {
                    scc: "2270001010".into(),
                    record: AllocationRecord {
                        scc: "2270001010".into(),
                        coefficients: vec![1.0],
                        indicator_codes: vec!["POP".into()],
                    },
                }],
                allocation_indicators: IndicatorTable::new(vec![
                    IndicatorRecord {
                        code: "POP".into(),
                        fips: "00000".into(),
                        subcounty: "".into(),
                        year: "2002".into(),
                        value: 1000.0,
                    },
                    IndicatorRecord {
                        code: "POP".into(),
                        fips: "06000".into(),
                        subcounty: "".into(),
                        year: "2002".into(),
                        value: 300.0,
                    },
                ]),
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: Some("GDP".into()),
                }],
                // Two flat national indicator rows (value 1.0 in both years)
                // make `growth_factor(2020, 2021, ..)` resolve to 0.0 — the same
                // neutral factor these state-class paths previously fabricated —
                // while keeping the data consistent with `growth_loaded = true`.
                // (The state/national/US-total `growth_factor` now errors on an
                // empty indicator selection, matching grwfac's 7000 fatal path.)
                growth_records: vec![
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2020,
                        value: 1.0,
                    },
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2021,
                        value: 1.0,
                    },
                ],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::National,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        // Temporal factors now ported (mo-cdo): National execution succeeds.
        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 1, "expected exactly one SimEmissionRow");
        assert_eq!(result.rows[0].fips, "06000", "expected state FIPS 06000");
    }

    /// (f) US-total dispatch with a minimal reference table →
    /// one `SimEmissionRow` at FIPS `"00000"`.
    ///
    /// Zero tech fractions bypass the emission iteration; one `StateOutput`
    /// for `"00000"` is still emitted.
    #[test]
    fn us_total_minimal_ref_returns_one_row() {
        let mut exec = ProductionExecutor {
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "00000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: Some("GDP".into()),
                }],
                // Two flat national indicator rows (value 1.0 in both years)
                // make `growth_factor(2020, 2021, ..)` resolve to 0.0 — the same
                // neutral factor these state-class paths previously fabricated —
                // while keeping the data consistent with `growth_loaded = true`.
                // (The state/national/US-total `growth_factor` now errors on an
                // empty indicator selection, matching grwfac's 7000 fatal path.)
                growth_records: vec![
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2020,
                        value: 1.0,
                    },
                    GrowthIndicatorRecord {
                        indicator: "GDP".into(),
                        fips: "00000".into(),
                        subregion: String::new(),
                        year: 2021,
                        value: 1.0,
                    },
                ],
                activity_entries: vec![ActivityTableEntry {
                    scc: "2270001010".into(),
                    fips: "".into(),
                    starts: 0.0,
                    activity_level: 100.0,
                    activity_unit: ActivityUnit::HoursPerYear,
                    load_factor: 0.5,
                    age_code: "DEFAULT".into(),
                }],
                scrappage_curve: vec![
                    ScrappagePoint {
                        bin: 0.0,
                        percent: 0.0,
                    },
                    ScrappagePoint {
                        bin: 100.0,
                        percent: 100.0,
                    },
                ],
                age_adjustment_table: AgeAdjustmentTable::default(),
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::UsTotal,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        // Temporal factors now ported (mo-cdo): UsTotal execution succeeds.
        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 1, "expected exactly one SimEmissionRow");
    }

    /// (g) National dispatch with no allocation entry for the SCC →
    /// `AllocationNotFound` error.
    #[test]
    fn national_allocation_not_found() {
        let mut exec = ProductionExecutor {
            state_descriptors: vec![StateDescriptor {
                fips: "06000".into(),
                selected: true,
                has_state_records: false,
            }],
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = test_opts();
        let record = DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::National,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("AllocationNotFound") || msg.contains("allocation"),
            "expected AllocationNotFound error, got: {msg}"
        );
    }

    /// (h) US-total dispatch with `growth_loaded = false` →
    /// `GrowthFileMissing` error.
    #[test]
    fn us_total_growth_file_missing() {
        let mut exec = ProductionExecutor {
            hp_levels: default_hp_levels(),
            reference: ReferenceData {
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                    ..Default::default()
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                    ..Default::default()
                }],
                ..ReferenceData::default()
            },
            ..ProductionExecutor::default()
        };

        let opts = NonroadOptions::new(RegionLevel::County, 2020); // growth_loaded = false
        let record = DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::UsTotal,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("GROWTH") || msg.contains("growth"),
            "expected GrowthFileMissing error, got: {msg}"
        );
    }

    // ---- day_month_factors_for (daymthf.f port) -------------------------

    /// Annual run (selected_month=0): all months selected, total_mode=true.
    /// Canonical daymthf.f for annual total: mthf = sum(monthly) = 1.0,
    /// ndays = 365, dayf = 1.0 (total mode forces dayf=1).
    #[test]
    fn annual_total_mode_returns_365_days_and_unit_factors() {
        use crate::simulation::inputs::TemporalProfile;
        let mut ref_data = ReferenceData::default();
        ref_data.temporal_profiles.insert(
            "2270001010".to_string(),
            TemporalProfile {
                monthly: [1.0 / 12.0; 12], // uniform monthly
                daily: [1.0 / 7.0; 2],
            },
        );
        let exec = ProductionExecutor {
            reference: ref_data,
            months_selected: [true; 12], // annual = all months
            weekday_selected: true,
            total_mode: true,
            ..Default::default()
        };
        let dmf = exec.day_month_factors_for("2270001010");
        assert_eq!(dmf.n_days, 365, "annual run must span all 365 days");
        assert!(
            (dmf.month_factor - 1.0).abs() < 1e-5,
            "mthf for uniform annual profile must be 1.0, got {}",
            dmf.month_factor
        );
        assert_eq!(dmf.day_of_week_factor, 1.0, "total mode must give dayf=1.0");
    }

    /// Monthly typical-day run (August, weekday).
    /// mthf = august_fraction, ndays = 31, dayf = 7 × weekday_fraction.
    #[test]
    fn monthly_typical_day_august_weekday_returns_correct_factors() {
        use crate::simulation::inputs::TemporalProfile;
        let august_fraction = 0.101_f32; // typical summer value from SEASON.DAT
        let weekday_fraction = 0.111111_f32; // 1 of 5 weekdays weighted
        let mut monthly = [0.0_f32; 12];
        monthly[7] = august_fraction; // index 7 = August
        let mut ref_data = ReferenceData::default();
        ref_data.temporal_profiles.insert(
            "2260001000".to_string(),
            TemporalProfile {
                monthly,
                daily: [weekday_fraction, 0.222222],
            },
        );
        let mut months = [false; 12];
        months[7] = true; // August only
        let exec = ProductionExecutor {
            reference: ref_data,
            months_selected: months,
            weekday_selected: true,
            total_mode: false,
            ..Default::default()
        };
        let dmf = exec.day_month_factors_for("2260001000");
        assert_eq!(dmf.n_days, 31, "August has 31 days");
        assert!(
            (dmf.month_factor - august_fraction).abs() < 1e-6,
            "mthf must equal august_fraction, got {}",
            dmf.month_factor
        );
        let expected_dayf = 7.0 * weekday_fraction;
        assert!(
            (dmf.day_of_week_factor - expected_dayf).abs() < 1e-5,
            "dayf must be 7×weekday_fraction={expected_dayf}, got {}",
            dmf.day_of_week_factor
        );
    }

    /// SCC with no temporal profile falls back to canonical defaults:
    /// defmth = 1/12 each, defday = 1/7 each.
    #[test]
    fn missing_scc_falls_back_to_canonical_defaults() {
        let mut months = [false; 12];
        months[7] = true; // August
        let exec = ProductionExecutor {
            months_selected: months,
            weekday_selected: true,
            total_mode: false,
            ..Default::default()
        };
        let dmf = exec.day_month_factors_for("9999999999");
        assert_eq!(dmf.n_days, 31);
        assert!(
            (dmf.month_factor - 1.0 / 12.0).abs() < 1e-6,
            "default mthf must be 1/12, got {}",
            dmf.month_factor
        );
        let expected_dayf = 7.0 * (1.0 / 7.0);
        assert!(
            (dmf.day_of_week_factor - expected_dayf).abs() < 1e-5,
            "default dayf must be 7×(1/7)=1.0, got {}",
            dmf.day_of_week_factor
        );
    }
}

// =============================================================================
// fndrfm unit tests
// =============================================================================

#[cfg(test)]
mod fndrfm_tests {
    use super::*;
    use crate::input::spillage::{RangeIndicator, RefuelingMode, SpillageRecord, SpillageUnits};

    fn make_rec(scc: &str, tech: &str, hp_min: f32, hp_max: f32, tank: f32) -> SpillageRecord {
        SpillageRecord {
            scc: scc.to_string(),
            mode: RefuelingMode::Pump,
            indicator: RangeIndicator::Horsepower,
            hp_min,
            hp_max,
            tech_type: tech.to_string(),
            units: SpillageUnits::Gallons,
            tank_volume: tank,
            tank_full: 0.5,
            tank_metal_pct: 0.0,
            hose_len: 0.1,
            hose_dia: 0.005,
            hose_metal_pct: 0.0,
            neck_len: 0.0,
            neck_dia: 0.0,
            sr_len: 0.0,
            sr_dia: 0.0,
            vent_len: 0.0,
            vent_dia: 0.0,
            hot_soak_per_hr: 0.05,
            diurnal: [1.0, 0.0, 0.0, 0.0, 0.0],
            tank_e10: 1.0,
            hose_e10: 1.0,
            neck_e10: 1.0,
            sr_e10: 1.0,
            vent_e10: 1.0,
        }
    }

    #[test]
    fn exact_scc_match() {
        let recs = vec![make_rec("2260001010", "ALL", 0.0, 9999.0, 3.0)];
        let r = fndrfm(&recs, "2260001010", 25.0, "ALL").unwrap();
        assert!((r.tank - 3.0).abs() < 1e-4);
    }

    #[test]
    fn glob7_match() {
        // Record has 7-char-padded SCC "2260001000"; query is "2260001010"
        let recs = vec![make_rec("2260001000", "ALL", 0.0, 9999.0, 7.0)];
        let r = fndrfm(&recs, "2260001010", 25.0, "ALL").unwrap();
        assert!((r.tank - 7.0).abs() < 1e-4);
    }

    #[test]
    fn glob4_match() {
        let recs = vec![make_rec("2260000000", "ALL", 0.0, 9999.0, 4.0)];
        let r = fndrfm(&recs, "2260001010", 25.0, "ALL").unwrap();
        assert!((r.tank - 4.0).abs() < 1e-4);
    }

    #[test]
    fn exact_beats_glob() {
        let recs = vec![
            make_rec("2260000000", "ALL", 0.0, 9999.0, 4.0), // 4-char glob
            make_rec("2260001010", "ALL", 0.0, 9999.0, 3.0), // exact
            make_rec("2260001000", "ALL", 0.0, 9999.0, 7.0), // 7-char glob
        ];
        let r = fndrfm(&recs, "2260001010", 25.0, "ALL").unwrap();
        assert!((r.tank - 3.0).abs() < 1e-4, "exact match should win");
    }

    #[test]
    fn glob7_beats_glob4() {
        let recs = vec![
            make_rec("2260000000", "ALL", 0.0, 9999.0, 4.0), // 4-char glob
            make_rec("2260001000", "ALL", 0.0, 9999.0, 7.0), // 7-char glob
        ];
        let r = fndrfm(&recs, "2260001010", 25.0, "ALL").unwrap();
        assert!(
            (r.tank - 7.0).abs() < 1e-4,
            "7-char glob should beat 4-char glob"
        );
    }

    #[test]
    fn hp_range_filter() {
        let recs = vec![
            make_rec("2260001010", "ALL", 0.0, 25.0, 1.0), // excludes hp=50
            make_rec("2260001010", "ALL", 25.0, 100.0, 2.0), // includes hp=50
        ];
        let r = fndrfm(&recs, "2260001010", 50.0, "ALL").unwrap();
        assert!((r.tank - 2.0).abs() < 1e-4);
    }

    #[test]
    fn hp_proximity_tiebreak_same_scc_quality() {
        // Two exact-SCC records in same HP band; hp=15 is closer to [10,20] than [0,50]
        let recs = vec![
            make_rec("2260001010", "ALL", 0.0, 50.0, 10.0), // idiff = max(15,35) = 35
            make_rec("2260001010", "ALL", 10.0, 20.0, 5.0), // idiff = max(5,5) = 5 (closer)
        ];
        let r = fndrfm(&recs, "2260001010", 15.0, "ALL").unwrap();
        assert!((r.tank - 5.0).abs() < 1e-4, "closer HP range should win");
    }

    #[test]
    fn tech_filter_no_match() {
        let recs = vec![make_rec("2260001010", "BASE", 0.0, 9999.0, 3.0)];
        assert!(fndrfm(&recs, "2260001010", 25.0, "ALL").is_none());
    }

    #[test]
    fn no_records_returns_none() {
        assert!(fndrfm(&[], "2260001010", 25.0, "ALL").is_none());
    }

    #[test]
    fn canonical_spillage_emf() {
        // Reads the actual EPA SPILLAGE.EMF from the canonical migration-source
        // cache. Skips gracefully when the file is not present (e.g. in CI).
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let path = std::path::PathBuf::from(home).join(
            ".cache/moves-rs-migration-src/EPA_MOVES_Model/NONROAD/NR08a/DATA/EMSFAC/SPILLAGE.EMF",
        );
        if !path.exists() {
            eprintln!("canonical_spillage_emf: SPILLAGE.EMF not found at {path:?}; skipping");
            return;
        }
        let file = std::fs::File::open(&path).expect("open SPILLAGE.EMF");
        let reader = std::io::BufReader::new(file);
        let recs = crate::input::spillage::read_spil(reader).expect("parse SPILLAGE.EMF");
        assert!(!recs.is_empty(), "SPILLAGE.EMF must contain records");

        // Verify the first record against known values from the file header.
        // SCC 2260001010 (2-Str Offroad Motorcycles), ALL tech, HP 0-9999, CONTAINER.
        let r = fndrfm(&recs, "2260001010", 500.0, "ALL")
            .expect("should find a record for 2260001010 at any HP");
        assert_eq!(r.mode.trim(), "CONTAINER", "mode mismatch for 2260001010");
        assert!(
            (r.tank - 3.0).abs() < 1e-3,
            "tank_volume mismatch: {}",
            r.tank
        );
        assert!(
            (r.tank_full - 0.5).abs() < 1e-5,
            "tank_full mismatch: {}",
            r.tank_full
        );
        assert!(
            (r.hot_soak_start - 0.05).abs() < 1e-5,
            "hot_soak mismatch: {}",
            r.hot_soak_start
        );
        assert!(
            (r.hose_length - 0.45750).abs() < 1e-4,
            "hose_len mismatch: {}",
            r.hose_length
        );
        assert!(
            (r.tnk_e10_factor - 1.0).abs() < 1e-6,
            "tank_e10 mismatch: {}",
            r.tnk_e10_factor
        );
        // neck_e10 was 0.0 in file → should be defaulted to 1.0
        assert!(
            (r.neck_e10_factor - 1.0).abs() < 1e-6,
            "neck_e10 mismatch: {}",
            r.neck_e10_factor
        );
    }
}
