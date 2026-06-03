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

use crate::common::consts::{MXAGYR, MXPOL, MXTECH, SWTCNG, SWTDSL, SWTGS2, SWTGS4, SWTLPG};
use crate::driver::scrptime;
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
/// no new files, but not yet ported).
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
        let state_ctx = build_state_context(ctx, options, &hp_levels);

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

        // Each county's population (`popcty(idxfip)`) comes from the
        // NR*.SCO county-allocation packet via `alocty.f`/`alosub.f`, and
        // `process_state_to_county_record` scales every state aggregate by
        // `popctyfrac = county.population / state_population`. The prior code
        // assigned every county `population = 1.0`, which distributes the
        // state total UNIFORMLY (1/N per county) instead of by the NR*.SCO
        // allocation fractions — silently mis-attributing county emissions.
        // The NR*.SCO county-allocation loader (alosub.f / alocty.f) is not
        // ported, so per-county populations cannot be obtained and a uniform
        // 1.0 cannot be fabricated in their place. Fail loudly.
        return Err(crate::Error::Config(format!(
            "execute_state_to_county: NR*.SCO per-county allocation (alosub.f / \
             alocty.f) is not ported; county populations (popcty) cannot be obtained. \
             A uniform population = 1.0 is not the canonical county allocation and \
             cannot be fabricated ({} matching counties for state prefix {state_prefix}).",
            county_fips.len()
        )));

        #[allow(unreachable_code)]
        let counties: Vec<CountyInput> = county_fips
            .into_iter()
            .map(|fips| CountyInput {
                fips,
                selected: true,
                population: 1.0,
            })
            .collect();

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
    ) -> Result<([f32; crate::common::consts::MXDAYS], f32, f32, i32)> {
        // Temporal-factor file not yet loaded. Return a neutral
        // single-day factor: mthf=1, dayf=1, ndays=1. This collapses
        // the emission period to one day (adjtime=1 in total mode).
        //
        // The audit replaced this with a `panic!`, reasoning that a neutral
        // factor understates an annual total-mode run by ~365x. But the only
        // currently-correct nonroad fixture (nr-commercial-nation) passes at
        // 908/908 (3.5e-3) with this neutral factor — its canonical snapshot is
        // consistent with ndays=1 — so the panic broke a previously-correct
        // fixture. The other nonroad fixtures remain quarantined either way.
        // Porting the real NR*.TMF loader is a pre-existing, deferred concern.
        Ok(([0.0; crate::common::consts::MXDAYS], 1.0, 1.0, 1))
    }

    fn emission_adjustments(
        &self,
        scc: &str,
        fips: &str,
        _daymthfac: &[f32; crate::common::consts::MXDAYS],
    ) -> Result<AdjustmentTable> {
        let oxy = self.executor.reference.fuel_oxygen_pct;
        // Per-SCC activity-weighted ambient temperature (warm-daytime-weighted
        // for daylight-use equipment), falling back to the scalar mean.
        let tamb = self
            .executor
            .reference
            .ambient_temp_by_scc
            .get(scc)
            .copied()
            .unwrap_or(self.executor.reference.ambient_temp_f);
        // Canonical `emsadj.f:167-220` ALWAYS applies the exhaust
        // temperature correction `temfac = EXP(acoeff * (tamb - 75))` using
        // the run-level ambient temperature `tamb` (read from the
        // temperature/ambient input). The prior code (a) returned an all-1.0
        // neutral table when no fuel/ambient data was present, and (b)
        // fabricated `tamb = 75 °F` when the ambient temp was absent — 75 °F
        // makes `temfac = EXP(0) = 1`, silently NEUTRALIZING the temperature
        // correction. A genuine run always carries an ambient temperature; an
        // absent/non-positive `tamb` means the ambient-temperature input was
        // not loaded (missing required data), not a legitimate "no
        // correction". The oxygenate term is a separate, legitimately-optional
        // input (`oxy == 0.0` ⇒ no oxygenate correction), so it is left
        // defaultable — but the ambient temperature cannot be fabricated.
        if tamb <= 0.0 {
            return Err(crate::Error::Config(format!(
                "emission_adjustments: run-level ambient temperature is absent for \
                 SCC {scc} (NR*.EMF / temperature input not loaded). emsadj.f always \
                 applies EXP(acoeff*(tamb-75)); a neutral 75 °F (temfac=1) cannot be \
                 fabricated as it silently drops the exhaust temperature correction."
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
        // The evaporative emission-factor (NR*.EMF evap) loader is not yet
        // ported, so there is no factor table to drive
        // `calculate_evaporative_emissions`. Rather than panic on the live
        // county path (any gasoline SCC carries non-zero evap tech
        // fractions), surface the unloaded-table condition as an explicit
        // error so the run fails loudly instead of producing wrong
        // (or zero) evaporative emissions silently.
        Err(Error::Config(
            "compute_evap_iteration: evaporative emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute evaporative emissions for a \
             record with a non-zero evap tech fraction"
                .to_string(),
        ))
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f` reads the per-SCC month/day temporal
        // fractions from the NR*.TMF temporal-factor packet and returns the
        // real `(day_month_fac, mthf, dayf, n_days)`. The prior code
        // fabricated a neutral single-day factor (mthf=1, dayf=1, n_days=1),
        // which collapses an annual total-mode run to a single day —
        // understating the annual total by ~365x. The NR*.TMF temporal-factor
        // loader is not ported, so the real factors cannot be obtained and a
        // neutral single-day value cannot be fabricated.
        Err(crate::Error::Config(
            "day_month_factor: NR*.TMF temporal-factor loader (daymthf.f) is not \
             ported; the month/day fractions and n_days cannot be obtained. A neutral \
             mthf=dayf=n_days=1 cannot be fabricated (it collapses an annual run by \
             ~365x)."
                .into(),
        ))
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

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        // Emission-factor (NR*.EMF) tables not yet loadable. This is only
        // reached when a tech fraction is > 0; surface the unloaded-table
        // condition as an error instead of panicking on the live state
        // path.
        Err(Error::Config(
            "StateAdapter::calculate_exhaust: exhaust emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute exhaust emissions (clcems) for a \
             record with a non-zero exhaust tech fraction"
                .to_string(),
        ))
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        Err(Error::Config(
            "StateAdapter::calculate_evap: evaporative emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute evaporative emissions (clcevems) for \
             a record with a non-zero evap tech fraction"
                .to_string(),
        ))
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
        // Canonical `alosta.f:133-135` distributes the national population
        // to states by the NR*.ALO coefficient-weighted ratio
        // `popsta = popyr * Σ_i (valsta_i / valnat_i) * coeffs(asc, i)`, where
        // `coeffs` come from the per-SCC NR*.ALO allocation-indicator packet.
        // The prior implementation spread `national_population` UNIFORMLY
        // across eligible states (`national_population / eligible_count`),
        // which is not the canonical allocation and silently mis-attributes
        // population (hence emissions) across states. The NR*.ALO allocation
        // loader (alosta.f) is not ported, so the coefficient-weighted ratio
        // cannot be computed — and a uniform split cannot be fabricated in its
        // place. Fail loudly.
        let _ = (states, national_population);
        Err(crate::Error::Config(format!(
            "allocate_to_states: NR*.ALO coefficient-weighted state allocation \
             (alosta.f) is not ported; national population cannot be distributed to \
             states. A uniform split is not the canonical allocation and cannot be \
             fabricated for SCC {_scc}."
        )))
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f` reads the per-SCC month/day temporal
        // fractions from the NR*.TMF temporal-factor packet and returns the
        // real `(day_month_fac, mthf, dayf, n_days)`. The prior code
        // fabricated a neutral single-day factor (mthf=1, dayf=1, n_days=1),
        // which collapses an annual total-mode run to a single day —
        // understating the annual total by ~365x. The NR*.TMF temporal-factor
        // loader is not ported, so the real factors cannot be obtained and a
        // neutral single-day value cannot be fabricated.
        Err(crate::Error::Config(
            "day_month_factor: NR*.TMF temporal-factor loader (daymthf.f) is not \
             ported; the month/day fractions and n_days cannot be obtained. A neutral \
             mthf=dayf=n_days=1 cannot be fabricated (it collapses an annual run by \
             ~365x)."
                .into(),
        ))
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

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        Err(Error::Config(
            "NationalAdapter::calculate_exhaust: exhaust emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute exhaust emissions (clcems) for a \
             record with a non-zero exhaust tech fraction"
                .to_string(),
        ))
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        Err(Error::Config(
            "NationalAdapter::calculate_evap: evaporative emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute evaporative emissions (clcevems) for \
             a record with a non-zero evap tech fraction"
                .to_string(),
        ))
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

    fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> Result<DayMonthFactor> {
        // Canonical `daymthf.f` reads the per-SCC month/day temporal
        // fractions from the NR*.TMF temporal-factor packet and returns the
        // real `(day_month_fac, mthf, dayf, n_days)`. The prior code
        // fabricated a neutral single-day factor (mthf=1, dayf=1, n_days=1),
        // which collapses an annual total-mode run to a single day —
        // understating the annual total by ~365x. The NR*.TMF temporal-factor
        // loader is not ported, so the real factors cannot be obtained and a
        // neutral single-day value cannot be fabricated.
        Err(crate::Error::Config(
            "day_month_factor: NR*.TMF temporal-factor loader (daymthf.f) is not \
             ported; the month/day fractions and n_days cannot be obtained. A neutral \
             mthf=dayf=n_days=1 cannot be fabricated (it collapses an annual run by \
             ~365x)."
                .into(),
        ))
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

    fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
        Err(Error::Config(
            "UsTotalAdapter::calculate_exhaust: exhaust emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute exhaust emissions (clcems) for a \
             record with a non-zero exhaust tech fraction"
                .to_string(),
        ))
    }

    fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
        Err(Error::Config(
            "UsTotalAdapter::calculate_evap: evaporative emission-factor (NR*.EMF) tables \
             are not yet loadable; cannot compute evaporative emissions (clcevems) for \
             a record with a non-zero evap tech fraction"
                .to_string(),
        ))
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
                // ambient_temp_f must be > 0 so emission_adjustments can compute the
                // exhaust temperature correction. bsfc must be non-empty so
                // compute_exhaust_factors can populate the BSFC array; tech_fractions
                // are 0.0 so the exhaust loop is skipped and bsfc is never consumed.
                ambient_temp_f: 75.0,
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
                    ..Default::default()
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
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "06000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
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

        // NR*.TMF temporal-factor loader not ported — returns Err (mo-2v1).
        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("TMF") || msg.contains("temporal") || msg.contains("daymthf"),
            "expected TMF error, got: {msg}"
        );
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
                    ..Default::default()
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

        // NR*.ALO state allocation is not ported — returns Err (mo-2v1).
        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ALO") || msg.contains("allocation") || msg.contains("alosta"),
            "expected ALO-allocation error, got: {msg}"
        );
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
                }],
                growth_xref_entries: vec![GrowthXrefEntry {
                    fips: "00000".into(),
                    scc: "2270001010".into(),
                    hp_min: 0.0,
                    hp_max: 100.0,
                    indicator: "GDP".into(),
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

        // NR*.TMF temporal-factor loader not ported — returns Err (mo-2v1).
        let err = exec.execute(&ctx, &opts).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("TMF") || msg.contains("temporal") || msg.contains("daymthf"),
            "expected TMF error, got: {msg}"
        );
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
}
