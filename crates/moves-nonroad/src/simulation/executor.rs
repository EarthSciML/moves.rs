//! The geography-execution seam: the boundary between the *driver
//! loop* (this task) and the *numerical evaluation* of NONROAD's six
//! geography routines.
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
//! callback contexts is itself substantial — the `geography` module
//! flagged it as deferred work — so the driver loop talks to the
//! routines through one narrow trait, [`GeographyExecutor`], rather
//! than wiring all four callback families inline.
//!
//! This keeps the deliverables cleanly separable:
//!
//! - **This task** owns the driver loop, the [`GeographyExecutor`]
//!   contract, the [`DispatchContext`] / [`GeographyExecution`] data
//!   shapes, and a reference executor ([`PlanRecordingExecutor`]).
//! - **A following increment** owns the production
//!   [`GeographyExecutor`] that builds the callback contexts from
//!   [`NonroadInputs`](super::NonroadInputs) reference data and calls
//!   the real routines.
//!
//! The same seam is the **instrumentation hook** the NONROAD
//! numerical-fidelity harness needs (Tasks 115/116): an executor that
//! records its [`DispatchContext`] inputs and [`GeographyExecution`]
//! outputs captures the port-side intermediate state to diff against
//! the gfortran reference. [`PlanRecordingExecutor`] is the minimal
//! shape of such an instrumenting executor.

use crate::driver::{Dispatch, DriverRecord};
use crate::emissions::exhaust::FuelKind;
use crate::Result;

use super::options::NonroadOptions;
use super::outputs::SimEmissionRow;

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
/// | `find_exhaust_tech` | `fndtch(asccod, hpval, year)` | Exhaust tech-type fractions (`tchfrc`, `tectyp`) from NR\*.EF emission-factor files | — (table lookup) | **⚠ NOT YET LOADABLE** — tech-fraction loader not ported |
/// | `find_evap_tech` | `fndevtch(asccod, hpval, year)` | Evap tech-type fractions (`evtchfrc`, `evtectyp`) from NR\*.EF files | — (table lookup) | **⚠ NOT YET LOADABLE** |
///
/// ## Activity records
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_activity` | `fndact(asccod, fipin, hpval)` | Activity records (`actlev`, `faclod`, `iactun`, `actage`, `starts`) from NR\*.ACT files | — | **⚠ NOT YET LOADABLE** — activity-file loader not ported |
/// | `activity_record` | reads `actlev(idxact)`, `faclod(idxact)`, `iactun(idxact)`, `actage(idxact)`, `starts(idxact)` | Same NR\*.ACT records as `find_activity` | — | **⚠ NOT YET LOADABLE** |
///
/// ## Growth cross-reference and growth factors
///
/// | Method | Fortran | Backing table | Math module | Status |
/// |--------|---------|---------------|-------------|--------|
/// | `find_growth_xref` | `fndgxf(fipin, asccod, hpval)` | Growth cross-reference table (`gxfdat`) from NR\*.GRW indicator files | — | **⚠ NOT YET LOADABLE** — growth xref loader not ported |
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
/// `ProductionExecutor` can produce non-skipped results:
///
/// 1. **NR\*.EF** — exhaust and evap tech-type fractions (`fndtch`,
///    `fndevtch`) and emission-factor records (`emfclc`, `evemfclc`,
///    `emsadj`).
/// 2. **NR\*.ACT** — activity records (`fndact`, `activity_record`).
/// 3. **NR\*.GRW** — growth cross-reference table (`fndgxf`).
/// 4. **NR\*.TMF** — temporal day/month factor table (`daymthf`).
/// 5. **NR\*.SPL** — refueling/spillage-mode records (`fndrfm`).
/// 6. **NR\*.SCO** — subcounty allocation coefficients.
/// 7. **NR\*.ALO** — national-to-state allocation coefficients.
/// 8. **`alosub` / `alosta` ports** — allocation math (pure computation,
///    no new files, but not yet ported).
///
/// All `population::` and `emissions::` math modules referenced above
/// are already ported and available.
#[derive(Debug, Default)]
pub struct ProductionExecutor {
    /// Exhaust tech-type fractions from NR*.EF files. **⚠ NOT YET LOADABLE.**
    pub exhaust_tech_fractions: Vec<u8>,
    /// Evap tech-type fractions from NR*.EF files. **⚠ NOT YET LOADABLE.**
    pub evap_tech_fractions: Vec<u8>,
    /// Emission-factor records from NR*.EMF files. **⚠ NOT YET LOADABLE.**
    pub emission_factors: Vec<u8>,
    /// Activity records from NR*.ACT files. **⚠ NOT YET LOADABLE.**
    pub activity_records: Vec<u8>,
    /// Growth cross-reference and growth-factor stream from NR*.GRW files.
    /// **⚠ NOT YET LOADABLE.**
    pub growth_xref: Vec<u8>,
    /// Day/month temporal factors from NR*.TMF files. **⚠ NOT YET LOADABLE.**
    pub temporal_factors: Vec<u8>,
    /// Refueling/spillage-mode records from NR*.SPL files. **⚠ NOT YET LOADABLE.**
    pub spillage_records: Vec<u8>,
    /// National-to-state allocation coefficients from NR*.ALO files.
    /// **⚠ NOT YET LOADABLE.**
    pub national_allocation: Vec<u8>,
    /// Subcounty allocation coefficients from NR*.SCO files. **⚠ NOT YET LOADABLE.**
    pub subcounty_allocation: Vec<u8>,
    /// Retrofit records from NR*.RFT files (population::retrofit::RetrofitRecord).
    pub retrofit_records: Vec<crate::population::RetrofitRecord>,
}

impl ProductionExecutor {
    /// Create a `ProductionExecutor` with no reference data loaded.
    ///
    /// All reference-data fields are empty; every dispatch returns
    /// [`GeographyExecution::skipped`] until the loaders are implemented
    /// (see the struct-level doc for the complete list of missing loaders).
    ///
    /// # Examples
    ///
    /// ```
    /// use moves_nonroad::simulation::ProductionExecutor;
    ///
    /// let executor = ProductionExecutor::new();
    /// assert!(executor.exhaust_tech_fractions.is_empty());
    /// assert!(executor.retrofit_records.is_empty());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    fn execute_county(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }

    fn execute_subcounty(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }

    fn execute_state_to_county(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }

    fn execute_state_from_national(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }

    fn execute_national(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }

    fn execute_us_total(
        &mut self,
        _ctx: &DispatchContext<'_>,
        _options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        Ok(GeographyExecution::skipped())
    }
}

impl GeographyExecutor for ProductionExecutor {
    fn execute(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        let result = match ctx.dispatch {
            Dispatch::County => self.execute_county(ctx, options)?,
            Dispatch::Subcounty => self.execute_subcounty(ctx, options)?,
            Dispatch::StateToCounty => self.execute_state_to_county(ctx, options)?,
            Dispatch::StateFromNational => self.execute_state_from_national(ctx, options)?,
            Dispatch::National => self.execute_national(ctx, options)?,
            Dispatch::UsTotal => self.execute_us_total(ctx, options)?,
        };
        if !result.skipped {
            todo!("non-skipped execution paths are not yet implemented");
        }
        Ok(result)
    }
}

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
        let mut exec = ProductionExecutor::new();
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
}
