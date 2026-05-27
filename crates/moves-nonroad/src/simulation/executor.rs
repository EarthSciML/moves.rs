//! The geography-execution seam: the boundary between the *driver
//! loop* and the *numerical evaluation* of NONROAD's six geography
//! routines.
//!
//! # Why a seam
//!
//! `nonroad.f`'s record loop ([`plan_scc_group`](crate::driver::plan_scc_group))
//! decides *which* geography routine each population record dispatches
//! to. Task 113 ported that decision logic as a pure planner and named
//! its consumer explicitly:
//!
//! > the executor that runs each decision against the geography
//! > routines and the writers is the Task 117 integration layer.
//!
//! [`run_simulation`](super::run_simulation) *is* that executor — it
//! walks the planner's [`DriverStep`](crate::driver::DriverStep)s and
//! invokes a geography routine for each [`Dispatch`]. But the six
//! routines ([`process_county`](crate::geography::process_county),
//! [`process_national_record`](crate::geography::process_national_record),
//! …) are not uniform: they take four different callback traits, each
//! of which must be populated from the loaded emission-factor,
//! technology, activity, growth, and retrofit tables. Assembling those
//! callback contexts behind one narrow trait, [`GeographyExecutor`],
//! keeps the driver loop decoupled from the callback-context assembly.
//!
//! This module provides two executors:
//!
//! - **[`ProductionExecutor`]** — assembles the four callback traits
//!   ([`GeographyCallbacks`], [`StateCallbacks`], [`UsTotalCallbacks`],
//!   [`NationalCallbacks`]) from loaded reference-data tables and calls
//!   the real geography routines.
//! - **[`PlanRecordingExecutor`]** — records each dispatch and returns
//!   empty output; makes the driver loop exercisable without any
//!   reference data. It is also the minimal shape the NONROAD
//!   numerical-fidelity harness (Tasks 115/116) needs for capturing
//!   port-side intermediate state.

use crate::common::consts::{MXAGYR, MXPOL, MXTECH, SWTCNG, SWTDSL, SWTGS2, SWTGS4, SWTLPG};
use crate::driver::scrptime;
use crate::driver::{Dispatch, DriverRecord};
use crate::emissions::exhaust::{
    calculate_exhaust_emissions, ActivityUnit as ExhaustActivityUnit, DayRange, EmissionUnitCode,
    ExhaustCalcInputs, FuelKind, PollutantFilter,
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
    /// [`fuel_for_scc`](crate::driver::fuel_for_scc). `None` is the
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
    /// Propagates any [`Error`](crate::Error) the geography routine
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
///    [`run_simulation`](super::run_simulation) with a
///    `PlanRecordingExecutor` produces a
///    [`NonroadOutputs`](super::NonroadOutputs) whose
///    [`counters`](super::NonroadOutputs::counters) and
///    [`completion_message`](super::NonroadOutputs::completion_message)
///    are fully populated — the complete run *structure* — while
///    [`dispatches`](Self::dispatches) holds the ordered dispatch
///    plan. The orchestrator can inspect what a run *will* do before
///    paying for the numerics.
/// 2. **Instrumentation skeleton.** It is the minimal shape of the
///    recording executor the numerical-fidelity harness needs (see the
///    module docs): swap the empty [`GeographyExecution`] for the real
///    routine output and the recorder also captures port-side
///    intermediate state.
/// 3. **Test double.** Unit and integration tests assert the driver
///    loop's dispatch order and counters against
///    [`dispatches`](Self::dispatches) without standing up the
///    geography routines.
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
//  ActivityTableEntry, NationalAllocationEntry are defined in
//  super::inputs and re-imported above.)

// =============================================================================
// ProductionExecutor
// =============================================================================

/// Production [`GeographyExecutor`] that routes each [`DispatchContext`] to
/// the matching NONROAD geography routine by assembling the four callback
/// traits (`GeographyCallbacks`, `StateCallbacks`, `UsTotalCallbacks`,
/// `NationalCallbacks`) from loaded reference-data tables.
///
/// # Callback-surface audit (T1 / mo-5w1lc)
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
/// | `find_exhaust_tech` | `fndtch(asccod, hpval, year)` | Exhaust tech-type fractions (`tchfrc`, `tectyp`) from NR\*.EF emission-factor files | — (table lookup) | **available** via [`exhaust_tech_entries`](Self::exhaust_tech_entries) |
/// | `find_evap_tech` | `fndevtch(asccod, hpval, year)` | Evap tech-type fractions (`evtchfrc`, `evtectyp`) from NR\*.EF files | — (table lookup) | **available** via [`evap_tech_entries`](Self::evap_tech_entries) |
///
/// ## Activity records
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_activity` | `fndact(asccod, fipin, hpval)` | Activity records (`actlev`, `faclod`, `iactun`, `actage`, `starts`) from NR\*.ACT files | — | **available** via [`activity_entries`](Self::activity_entries) |
/// | `activity_record` | reads `actlev(idxact)`, `faclod(idxact)`, `iactun(idxact)`, `actage(idxact)`, `starts(idxact)` | Same NR\*.ACT records as `find_activity` | — | **available** |
///
/// ## Growth cross-reference and growth factors
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_growth_xref` | `fndgxf(fipin, asccod, hpval)` | Growth cross-reference table (`gxfdat`) from NR\*.GRW indicator files | — | **available** via [`growth_xref_entries`](Self::growth_xref_entries) |
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
/// | `compute_evap_factors` | `evemfclc(…)` | Evap emission-factor records from NR\*.EMF files | `emissions::evaporative::calculate_evaporative_factors` | **math ported**; evap EF loader **⚠ NOT YET LOADABLE** |
///
/// ## Emission calculators
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `compute_exhaust_iteration` / `calculate_exhaust` | `clcems(…)` | EF table (above) + activity records | `emissions::exhaust::calculate_exhaust_emissions` | **ported** (depends on unloaded EF + activity tables) |
/// | `compute_evap_iteration` / `calculate_evap` | `clcevems(…)` | Evap EF table + refueling/spillage data | `emissions::evaporative::calculate_evaporative_emissions` | **ported** (depends on unloaded evap EF + spillage tables) |
///
/// ## Refueling / spillage
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_refueling` | `fndrfm(asccod, hpval, tech)` | Refueling/spillage-mode table (`modspl`, `volspl`, etc.) from NR\*.SPL files | — | **⚠ NOT YET LOADABLE** — spillage-file loader not ported |
///
/// ## Allocation (subcounty and national)
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_allocation` (subcounty) | `fndasc(asccod, ascalo, nalorc)` | Subcounty allocation coefficients from NR\*.SCO files | — | **⚠ NOT YET LOADABLE** |
/// | `allocate_subcounty` | `alosub(…)` | Same NR\*.SCO records | — (pure computation) | **⚠ NOT YET PORTED** |
/// | `find_allocation` (national) | `fndasc` national path | National-to-state allocation coefficients from NR\*.ALO files | — | **⚠ NOT YET LOADABLE** |
/// | `allocate_to_states` | `alosta(…)` | Same NR\*.ALO records | — (pure computation) | **⚠ NOT YET PORTED** |
///
/// # Summary: what blocks production execution
///
/// The following reference-data loaders must be ported before
/// `ProductionExecutor` can produce fully-populated results:
///
/// 1. **NR\*.EF** — emission-factor records (`emfclc`, `evemfclc`, `emsadj`).
/// 2. **NR\*.TMF** — temporal day/month factor table (`daymthf`).
/// 3. **NR\*.SPL** — refueling/spillage-mode records (`fndrfm`).
/// 4. **NR\*.SCO** — subcounty allocation coefficients.
/// 5. **NR\*.ALO** — national-to-state allocation coefficients.
/// 6. **`alosub` / `alosta` ports** — allocation math (pure computation,
///    no new files, but not yet ported).
///
/// Tech-type fractions (NR\*.EF), activity records (NR\*.ACT), growth
/// cross-reference and growth-factor data (NR\*.GRW), and retrofit
/// records (NR\*.RFT) are now carried as typed fields and populated by
/// the caller.
#[derive(Debug, Default)]
pub struct ProductionExecutor {
    /// County FIPS codes in the run (`fipcod(NCNTY)`). Used by
    /// [`CountyAdapter::find_fips`] to map a region-code to its slot
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
            // use_hours and disc_code are not in DriverRecord; use
            // defaults that keep scrptime in a well-defined state.
            use_hours: 1000.0,
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
            use_hours: 1000.0,
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
        let state_ctx = build_state_context(ctx, options, &hp_levels);

        // Build county list by filtering county_fips to those whose
        // 2-char state prefix matches the state FIPS in region_code.
        let state_prefix = ctx.record.region_code.get(..2).unwrap_or("");
        let counties: Vec<CountyInput> = self
            .county_fips
            .iter()
            .filter(|fips| fips.get(..2).unwrap_or("") == state_prefix)
            .map(|fips| CountyInput {
                fips: fips.clone(),
                selected: true,
                population: 1.0,
            })
            .collect();

        if counties.is_empty() {
            return Ok(GeographyExecution::skipped());
        }

        let mut adapter = StateAdapter::new(self);
        let output = process_state_to_county_record(&state_ctx, &counties, &mut adapter)?;
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

    fn find_exhaust_tech(&self, scc: &str, hp_avg: f32, _year: i32) -> Option<TechLookup> {
        let entry = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc && e.hp_min <= hp_avg && hp_avg <= e.hp_max)?;
        Some(TechLookup {
            scc_tech_index: 0,
            tech_names: entry.tech_names.clone(),
            tech_fractions: entry.tech_fractions.clone(),
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

    fn find_refueling(&self, _scc: &str, _hp_avg: f32, _tech_name: &str) -> Option<RefuelingData> {
        // Spillage-mode records not yet loaded; always miss.
        None
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

    fn day_month_factors(
        &self,
        _scc: &str,
        _fips: &str,
    ) -> ([f32; crate::common::consts::MXDAYS], f32, f32, i32) {
        // Temporal-factor file not yet loaded. Return a neutral
        // single-day factor: mthf=1, dayf=1, ndays=1. This collapses
        // the emission period to one day (adjtime=1 in total mode).
        ([0.0; crate::common::consts::MXDAYS], 1.0, 1.0, 1)
    }

    fn emission_adjustments(
        &self,
        _scc: &str,
        _fips: &str,
        _daymthfac: &[f32; crate::common::consts::MXDAYS],
    ) -> AdjustmentTable {
        // Emission adjustments require the EF tables; return a
        // no-adjustment (all 1.0 / empty) table for now.
        AdjustmentTable::new(crate::common::consts::MXDAYS)
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
        let indicator = self
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

        // 2. Select growth records for this indicator (clone to avoid
        //    borrow-checker conflict with the scrptime closure below).
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
        //    (grown to growth_year); the per-year adjustments from modyr.
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
        tech_names: &[String],
        _tech_fractions: &[f32],
        _model_year: i32,
        _year_index: usize,
        _record_index: usize,
    ) -> Result<ExhaustFactorsLookup> {
        // Look up per-tech BSFC from the reference entry for this SCC.
        // Emission-factor files are not yet loadable, so all EF arrays
        // stay zero; CO2 and SOx are rewritten from BSFC by
        // calculate_exhaust_emissions regardless of the EF values.
        let n_tech = tech_names.len().max(1);
        let bsfc_per_tech: Vec<f32> = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == scc)
            .map(|e| e.bsfc.clone())
            .unwrap_or_else(|| vec![0.0; n_tech]);
        let mut bsfc = vec![0.0_f32; MXAGYR * n_tech];
        for y in 0..MXAGYR {
            for (t, &v) in bsfc_per_tech.iter().enumerate().take(n_tech) {
                bsfc[y * n_tech + t] = v;
            }
        }
        Ok(ExhaustFactorsLookup {
            emission_factors: vec![0.0; MXAGYR * MXPOL * MXTECH],
            bsfc,
            unit_codes: vec![EmissionUnitCode::GramsPerHpHour; MXPOL * MXTECH],
            adetcf: vec![0.0; MXPOL * MXTECH],
            bdetcf: vec![0.0; MXPOL * MXTECH],
            detcap: vec![0.0; MXPOL * MXTECH],
        })
    }

    fn compute_evap_factors(
        &mut self,
        _scc: &str,
        _evap_tech_names: &[String],
        _evap_tech_fractions: &[f32],
        _model_year: i32,
        _year_index: usize,
        _record_index: usize,
    ) -> Result<EvapFactorsLookup> {
        Ok(EvapFactorsLookup::default())
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
        // The entry was already validated by model_year_and_agedist, so
        // unwrap_or is a safe fallback.
        let (load_factor, activity_unit_geo) = self
            .executor
            .reference
            .activity_entries
            .get(activity_index)
            .map(|e| (e.load_factor, e.activity_unit))
            .unwrap_or((0.5, ActivityUnit::HoursPerYear));

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
        let entry_fracs: &[f32] = self
            .executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == record.scc)
            .map(|e| e.tech_fractions.as_slice())
            .unwrap_or(&[]);
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

        let outputs = calculate_exhaust_emissions(&mut calc_inputs, &PollutantFilter::empty());

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
        _factors: &EvapFactorsLookup,
        _adjustments: &AdjustmentTable,
        _refueling: &RefuelingData,
        _scc_tech_index: usize,
        _tech_index: usize,
        _year_index: usize,
        _equipment_age: f32,
        _evap_tech_fraction: f32,
        _evap_tech_name: &str,
        _temporal_adjustment: f32,
        _starts_adjustment: f32,
        _model_year_fraction: f32,
        _activity_adjustment: f32,
        _population: f32,
        _n_days: i32,
        _fulbmy: f32,
    ) -> Result<EmissionsIterationResult> {
        todo!(
            "compute_evap_iteration: wire to calculate_evaporative_emissions \
             once NR*.EMF evap tables are loaded"
        )
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> DayMonthFactor {
        DayMonthFactor {
            day_month_fac: vec![0.0; crate::common::consts::MXDAYS],
            mthf: 1.0,
            dayf: 1.0,
            n_days: 1,
        }
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
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            // No growth data loaded; return zero (no growth).
            return Ok(0.0);
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
        Ok(RetrofitResult::default())
    }

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        // Emission-factor tables not yet loaded; return zero-emission result.
        // Only reached when tech_fraction > 0; tests use zero fractions.
        todo!(
            "StateAdapter::calculate_exhaust: wire to calculate_exhaust_emissions \
             once NR*.EMF tables are loaded"
        )
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        todo!(
            "StateAdapter::calculate_evap: wire to calculate_evaporative_emissions \
             once NR*.EMF evap tables are loaded"
        )
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
        _scc: &str,
        states: &[StateDescriptor],
        national_population: f32,
        _growth: f32,
    ) -> Result<StateAllocationOutcome> {
        let eligible_count = states
            .iter()
            .filter(|s| s.selected && !s.has_state_records)
            .count();
        let per_state = if eligible_count > 0 {
            national_population / eligible_count as f32
        } else {
            0.0
        };
        let mut populations = vec![0.0_f32; states.len()];
        for (i, state) in states.iter().enumerate() {
            if state.selected && !state.has_state_records {
                populations[i] = per_state;
            }
        }
        let used = populations.iter().any(|&p| p > 0.0);
        Ok(StateAllocationOutcome {
            populations,
            growth: vec![1.0; states.len()],
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> DayMonthFactor {
        DayMonthFactor {
            day_month_fac: vec![0.0; crate::common::consts::MXDAYS],
            mthf: 1.0,
            dayf: 1.0,
            n_days: 1,
        }
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
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            return Ok(0.0);
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
        Ok(RetrofitResult::default())
    }

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        todo!(
            "NationalAdapter::calculate_exhaust: wire to calculate_exhaust_emissions \
             once NR*.EMF tables are loaded"
        )
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        todo!(
            "NationalAdapter::calculate_evap: wire to calculate_evaporative_emissions \
             once NR*.EMF evap tables are loaded"
        )
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> DayMonthFactor {
        DayMonthFactor {
            day_month_fac: vec![0.0; crate::common::consts::MXDAYS],
            mthf: 1.0,
            dayf: 1.0,
            n_days: 1,
        }
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
            })?;
        let selected: Vec<GrowthIndicatorRecord> =
            select_for_indicator(&self.executor.reference.growth_records, &indicator)
                .into_iter()
                .cloned()
                .collect();
        if selected.is_empty() {
            return Ok(0.0);
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
        Ok(RetrofitResult::default())
    }

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        todo!(
            "UsTotalAdapter::calculate_exhaust: wire to calculate_exhaust_emissions \
             once NR*.EMF tables are loaded"
        )
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        todo!(
            "UsTotalAdapter::calculate_evap: wire to calculate_evaporative_emissions \
             once NR*.EMF evap tables are loaded"
        )
    }
}

// =============================================================================
// Helpers
// =============================================================================

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
        use_hours: 1000.0,
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
        use_hours: 1000.0,
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
        use_hours: 1000.0,
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
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06037".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
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
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
                }],
                growth_records: vec![],
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
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::StateToCounty,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 2, "expected one row per county");
        let fips_set: std::collections::HashSet<_> =
            result.rows.iter().map(|r| r.fips.as_str()).collect();
        assert!(fips_set.contains("06037"));
        assert!(fips_set.contains("06059"));
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
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
                }],
                growth_records: vec![],
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
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::StateFromNational,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(result.rows.len(), 1, "expected exactly one row for state");
        assert_eq!(result.rows[0].fips, "06000");
    }

    /// (e) National dispatch with one state and one allocation entry →
    /// one `SimEmissionRow` at the state FIPS.
    ///
    /// `state_index = 0` (national record) triggers `alosta`; the
    /// uniform-allocation stub gives state "06000" the full population.
    /// Zero tech fractions bypass the emission iteration; one `StateOutput`
    /// is still emitted per selected state.
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
                }],
                exhaust_tech_entries: vec![ExhaustTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["T1".into()],
                    tech_fractions: vec![0.0],
                    bsfc: vec![],
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
                }],
                growth_records: vec![],
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
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::National,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(
            result.rows.len(),
            1,
            "expected one row for the one selected state"
        );
        assert_eq!(result.rows[0].fips, "06000");
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
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "00000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
                }],
                growth_records: vec![],
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
        };
        let ctx = DispatchContext {
            dispatch: Dispatch::UsTotal,
            scc: "2270001010",
            fuel: Some(FuelKind::Diesel),
            record: &record,
            growth: None,
        };

        let result = exec.execute(&ctx, &opts).unwrap();
        assert!(!result.skipped, "expected non-skipped execution");
        assert_eq!(
            result.rows.len(),
            1,
            "expected exactly one row for US total"
        );
        assert_eq!(result.rows[0].fips, "00000");
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
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
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
                }],
                evap_tech_entries: vec![EvapTechEntry {
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    tech_names: vec!["EV1".into()],
                    tech_fractions: vec![0.0],
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
}
