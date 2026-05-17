//! [`ExecutionRunSpec`] — the runtime view of a [`RunSpec`].
//!
//! Ports `gov.epa.otaq.moves.master.framework.ExecutionRunSpec` (2.4k lines
//! in Java). The Java class is a singleton (`theExecutionRunSpec`) that
//! consults a thread-local MariaDB `Connection` for every initialisation
//! step; the Rust port is a value type that owns its [`RunSpec`] and
//! materialises every set it can derive purely from the spec in
//! [`ExecutionRunSpec::new`]. Database-dependent state (`fuel_years`,
//! `month_groups`, `regions`, `execution_locations`) is left empty here for
//! later phases to fill — Task 16 supplies the location iterator, Task 23
//! supplies `CalculatorContext` plumbing, and Task 24
//! (`InputDataManager`) populates the rest from the data plane.
//!
//! # What is and isn't ported in this commit
//!
//! **Ported (pure-RunSpec derivations):**
//!
//! * `target_processes`, `target_pollutants`, `pollutant_process_associations`,
//!   `target_pollutant_processes` — derived from
//!   [`RunSpec::pollutant_process_associations`].
//! * `years`, `months`, `days`, `hours`, `hour_days` — derived from
//!   [`RunSpec::timespan`]. `hour_days` is computed via the canonical MOVES
//!   formula `hour_id * 10 + day_id` rather than reading the `HourDay`
//!   table; the default DB encodes the same mapping (see
//!   [`ExecutionRunSpec::hour_day_id`]).
//! * `source_types`, `fuel_types`, `sectors` — derived from
//!   [`RunSpec::onroad_vehicle_selections`] and
//!   [`RunSpec::offroad_vehicle_selections`].
//! * Pollutant/process require-and-flag logic
//!   ([`ExecutionRunSpec::flag_required_pollutant_processes`] and
//!   [`ExecutionRunSpec::require`]): adds the refueling-process →
//!   running/start/extended-idle-exhaust dependency closure.
//! * Wildcard membership queries
//!   ([`ExecutionRunSpec::does_have_pollutant_and_process`] and variants).
//! * Final-aggregation decision logic
//!   ([`ExecutionRunSpec::should_do_final_aggregation`]).
//! * Class-name allow/save lists ([`ExecutionRunSpec::should_execute`],
//!   [`ExecutionRunSpec::should_save_data`]).
//!
//! **Not ported (left for downstream tasks):**
//!
//! * `ExecutionLocationProducer` integration — Task 16.
//! * MariaDB-style filter-table writes (`buildNonLocationFilterTables`,
//!   `buildLocationFilterTables`, `addIndexes`, `runAdditionalSetupScript`,
//!   `setupMacroExpander`) — Task 24 (`InputDataManager`) replaces these
//!   with `DataFrameStore` projections.
//! * `ModelYearMapper` — Task 23 (`CalculatorContext`).
//! * `AggregationSQLGenerator` glue (`retrofitSQLs`, `workerSQLs`,
//!   `outputProcessorSQLs`) — Task 25.
//! * TOG-speciation lumped-species expansion and
//!   `OnRoadRetrofitStrategy` integration — Phase 3 / Phase 6 calculator
//!   ports register through the `pollutants_needing_aggregation` and
//!   `pollutant_processes_needing_aggregation` hooks below.
//!
//! # Java singleton replaced by ownership
//!
//! Java exposes `ExecutionRunSpec.theExecutionRunSpec` and lets every
//! framework class reach into it. The Rust port is a plain value: hand it
//! to the calculator registry, the master loop, the input-data manager;
//! they all see the same state through their `&ExecutionRunSpec` or
//! `&mut ExecutionRunSpec` reference. No global singleton, no static state.

use std::collections::BTreeSet;

use moves_data::{
    EmissionProcess, PolProcessId, Pollutant, PollutantId, PollutantProcessAssociation, ProcessId,
    SourceTypeId,
};
use moves_runspec::{
    GeographicOutputDetail, Model, ModelDomain, ModelScale, OutputTimestep,
    PollutantProcessAssociation as RunSpecPollutantProcess, RunSpec,
};

use crate::execution_db::ExecutionLocation;
use crate::execution_location_producer::{ExecutionLocationProducer, GeographyTables};

/// Which engine combination drives the current run.
///
/// Ports the relevant pieces of `gov.epa.otaq.moves.common.Models.ModelCombination`.
/// Java's enum has more variants for transitional states; we only need the
/// three that determine runtime branching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelCombination {
    /// ONROAD only — the typical inventory run.
    Onroad,
    /// NONROAD only.
    Nonroad,
    /// Both ONROAD and NONROAD selected — supported in the data plane but
    /// the framework still iterates one model at a time.
    Both,
}

impl ModelCombination {
    /// Derive the combination from a [`RunSpec`].
    ///
    /// Empty selection defaults to [`ModelCombination::Onroad`], matching
    /// Java's behaviour when `Models.evaluateModels` is called on an empty
    /// list (treated as the legacy onroad default).
    #[must_use]
    pub fn from_run_spec(run_spec: &RunSpec) -> Self {
        let onroad = run_spec.models.contains(&Model::Onroad);
        let nonroad = run_spec.models.contains(&Model::Nonroad);
        match (onroad, nonroad) {
            (true, true) => Self::Both,
            (false, true) => Self::Nonroad,
            _ => Self::Onroad,
        }
    }
}

/// Runtime-time view of a [`RunSpec`]. Owns the spec and the set of derived
/// state used while the MasterLoop iterates.
///
/// See the [module docs](self) for what is and isn't populated in this
/// commit. The fields are deliberately public — `ExecutionRunSpec` is a
/// state record threaded between the registry, the master loop, the input
/// data manager, and the output processor. Encapsulating them behind
/// dozens of trivial getters added no value in the Java original and
/// wouldn't here either.
#[derive(Debug)]
pub struct ExecutionRunSpec {
    /// The RunSpec being executed.
    pub run_spec: RunSpec,

    /// `false` if advanced performance features disabled emission-calculator
    /// execution. Stays `true` until a caller toggles
    /// [`classes_not_to_execute`](Self::classes_not_to_execute) and re-checks.
    pub will_run_calculators: bool,

    /// The set of `EmissionProcess` ids targeted by the simulation.
    pub target_processes: BTreeSet<ProcessId>,

    /// The set of `Pollutant` ids targeted by the simulation.
    pub target_pollutants: BTreeSet<PollutantId>,

    /// The composite `polProcessID = pollutant_id * 100 + process_id` for
    /// every active pair. Calculator filtering against the chain DAG uses
    /// this set; it is always coherent with
    /// [`pollutant_process_associations`](Self::pollutant_process_associations).
    pub target_pollutant_processes: BTreeSet<PolProcessId>,

    /// `(pollutant, process)` pairs used in the simulation. May contain more
    /// entries than the user selected, because [`require`](Self::require)
    /// can add silently-required pairs to satisfy calculator chains.
    pub pollutant_process_associations: BTreeSet<PollutantProcessAssociation>,

    /// `(pollutant, process)` pairs that were added by
    /// [`require`](Self::require) for chain completeness rather than by
    /// the user. Phase 3 calculators consume their output normally; the
    /// final output processor strips these rows before writing.
    ///
    /// The Java original emits "delete from MOVESWorkerOutput where ..."
    /// SQL fragments for each silently-required pair — we track the pairs
    /// instead and leave the actual filtering to the output processor
    /// (Task 26), which queries this set.
    pub silently_required: BTreeSet<PollutantProcessAssociation>,

    /// Calendar years in the run.
    pub years: BTreeSet<u32>,

    /// Months in the run (1–12).
    pub months: BTreeSet<u32>,

    /// MOVES day ids (typically 2=weekday, 5=weekend; can be 1–7 with
    /// custom day aggregations).
    pub days: BTreeSet<u32>,

    /// Hours of day (1–24).
    pub hours: BTreeSet<u32>,

    /// `hourDayID = hour_id * 10 + day_id` for every `(hour, day)` pair in
    /// the active hours/days sets. Matches the canonical default-DB
    /// `HourDay` table content; see [`ExecutionRunSpec::hour_day_id`].
    pub hour_days: BTreeSet<u32>,

    /// Source-type ids selected for the run (onroad).
    pub source_types: BTreeSet<SourceTypeId>,

    /// Fuel-type ids — union of onroad and offroad fuel selections.
    pub fuel_types: BTreeSet<u16>,

    /// NONROAD sector ids selected for the run.
    pub sectors: BTreeSet<u16>,

    /// Fuel years for the active calendar years. Populated from the default
    /// DB `Year` table by Task 24; empty until then.
    pub fuel_years: BTreeSet<u32>,

    /// Month-group ids for the active months. Populated from the default
    /// DB `MonthOfAnyYear` table by Task 24; empty until then.
    pub month_groups: BTreeSet<u32>,

    /// Region ids the active counties belong to. Populated from
    /// `RegionCounty` by Task 24; always contains `0` (the wildcard
    /// region) once populated. Empty until Task 24.
    pub regions: BTreeSet<u32>,

    /// Iteration targets the MasterLoop sweeps over. Filled by Task 16's
    /// [`ExecutionLocationProducer`](crate); empty until then. Each
    /// member is expected to have all four ids populated (link
    /// granularity) — the same [`ExecutionLocation`] type Task 23
    /// introduced for per-iteration position snapshots is reused here as
    /// the set element, with all `Option<u32>` fields filled.
    ///
    /// Once populated, callers should run
    /// [`extract_location_details_from_execution_locations`](Self::extract_location_details_from_execution_locations)
    /// to project the `states`/`counties`/`zones`/`links` sets out of it.
    pub execution_locations: BTreeSet<ExecutionLocation>,

    /// State ids the run touches — derived from `execution_locations`.
    pub states: BTreeSet<u32>,

    /// County ids the run touches — derived from `execution_locations`.
    pub counties: BTreeSet<u32>,

    /// Zone ids the run touches — derived from `execution_locations`.
    pub zones: BTreeSet<u32>,

    /// Link ids the run touches — derived from `execution_locations`.
    pub links: BTreeSet<u32>,

    /// Pollutants whose calculator output benefits from post-run
    /// aggregation. Calculators register here at startup via
    /// [`pollutant_needs_aggregation`](Self::pollutant_needs_aggregation).
    pub pollutants_needing_aggregation: BTreeSet<PollutantId>,

    /// Pollutant/process pairs whose calculator output benefits from
    /// post-run aggregation.
    pub pollutant_processes_needing_aggregation: BTreeSet<PollutantProcessAssociation>,

    /// Fully-qualified class/calculator names the user has disabled.
    /// Empty in the Java default; populated through the `classesNotToExecute`
    /// RunSpec attribute when the parser starts respecting it.
    ///
    /// Ports `RunSpec.classesNotToExecute`. Stored here so the Rust
    /// `ExecutionRunSpec` carries the override without dragging the field
    /// onto the parsed [`RunSpec`] model — Task 12 will hoist it to the
    /// spec when the matching XML attribute is needed by the writer.
    pub classes_not_to_execute: BTreeSet<String>,

    /// Fully-qualified class/calculator names the user has flagged for
    /// data preservation. Mirrors `RunSpec.classesToSaveData`.
    pub classes_to_save_data: BTreeSet<String>,

    /// Internal flag — when true, the run keeps worker-side intermediate
    /// tables for inspection. Set by retrofit handling in
    /// [`initialize_before_iteration`](Self::initialize_before_iteration).
    pub should_keep_worker_databases: bool,
}

impl ExecutionRunSpec {
    /// Build a new [`ExecutionRunSpec`] from an owned [`RunSpec`].
    ///
    /// Runs the pure-data derivation pipeline:
    ///
    /// 1. Populate the pollutant/process sets from
    ///    `run_spec.pollutant_process_associations`.
    /// 2. Build the timespan sets (`years`, `months`, `days`, `hours`,
    ///    `hour_days`).
    /// 3. Build the vehicle-selection sets (`source_types`, `fuel_types`,
    ///    `sectors`).
    /// 4. Apply the static refueling-process dependency closure via
    ///    [`flag_required_pollutant_processes`](Self::flag_required_pollutant_processes).
    ///
    /// Database-dependent state (`fuel_years`, `month_groups`, `regions`,
    /// `execution_locations`, and the derived geographic sets) is left
    /// empty for Tasks 16 / 23 / 24 to fill in.
    #[must_use]
    pub fn new(run_spec: RunSpec) -> Self {
        let mut spec = Self {
            run_spec,
            will_run_calculators: true,
            target_processes: BTreeSet::new(),
            target_pollutants: BTreeSet::new(),
            target_pollutant_processes: BTreeSet::new(),
            pollutant_process_associations: BTreeSet::new(),
            silently_required: BTreeSet::new(),
            years: BTreeSet::new(),
            months: BTreeSet::new(),
            days: BTreeSet::new(),
            hours: BTreeSet::new(),
            hour_days: BTreeSet::new(),
            source_types: BTreeSet::new(),
            fuel_types: BTreeSet::new(),
            sectors: BTreeSet::new(),
            fuel_years: BTreeSet::new(),
            month_groups: BTreeSet::new(),
            regions: BTreeSet::new(),
            execution_locations: BTreeSet::new(),
            states: BTreeSet::new(),
            counties: BTreeSet::new(),
            zones: BTreeSet::new(),
            links: BTreeSet::new(),
            pollutants_needing_aggregation: BTreeSet::new(),
            pollutant_processes_needing_aggregation: BTreeSet::new(),
            classes_not_to_execute: BTreeSet::new(),
            classes_to_save_data: BTreeSet::new(),
            should_keep_worker_databases: false,
        };
        spec.populate_pollutant_processes();
        spec.populate_timespan();
        spec.populate_vehicle_selections();
        spec.flag_required_pollutant_processes();
        spec
    }

    /// MOVES canonical mapping `hour_id * 10 + day_id`. Matches every row in
    /// the default-DB `HourDay` table for `hour_id ∈ 1..=24` and
    /// `day_id ∈ 1..=9`. Bumped to `u32` since the product fits in a
    /// `SMALLINT` but Rust's preferred set element type for derived ids is
    /// `u32`.
    #[must_use]
    pub fn hour_day_id(hour_id: u32, day_id: u32) -> u32 {
        hour_id * 10 + day_id
    }

    fn populate_pollutant_processes(&mut self) {
        // Collect first so we don't hold an immutable borrow of `self.run_spec`
        // while calling `&mut self` helpers.
        let pairs: Vec<(u16, u16)> = self
            .run_spec
            .pollutant_process_associations
            .iter()
            .map(|ppa| (ppa.pollutant_id as u16, ppa.process_id as u16))
            .collect();
        for (pollutant_id, process_id) in pairs {
            self.insert_pollutant_process_owned(pollutant_id, process_id);
        }
    }

    /// Add `(pollutant_id, process_id)` to every coherent set. Internal —
    /// callers use [`require`](Self::require) which composes this with the
    /// silently-required tracking.
    fn insert_pollutant_process_owned(&mut self, pollutant_id: u16, process_id: u16) {
        let pid = PollutantId(pollutant_id);
        let proc = ProcessId(process_id);
        self.target_pollutants.insert(pid);
        self.target_processes.insert(proc);
        let assoc = PollutantProcessAssociation {
            pollutant_id: pid,
            process_id: proc,
        };
        self.pollutant_process_associations.insert(assoc);
        self.target_pollutant_processes.insert(assoc.polproc_id());
    }

    fn populate_timespan(&mut self) {
        let timespan = &self.run_spec.timespan;
        self.years.extend(timespan.years.iter().copied());
        self.months.extend(timespan.months.iter().copied());
        self.days.extend(timespan.days.iter().copied());

        let begin = timespan.begin_hour.unwrap_or(0);
        let end = timespan.end_hour.unwrap_or(0);
        if begin > 0 && end >= begin {
            for h in begin..=end {
                self.hours.insert(h);
            }
        }
        // Cartesian product hour × day → hourDayID.
        for &h in &self.hours {
            for &d in &self.days {
                self.hour_days.insert(Self::hour_day_id(h, d));
            }
        }
    }

    fn populate_vehicle_selections(&mut self) {
        let mc = ModelCombination::from_run_spec(&self.run_spec);
        match mc {
            ModelCombination::Onroad | ModelCombination::Both => {
                for sel in &self.run_spec.onroad_vehicle_selections {
                    self.source_types
                        .insert(SourceTypeId(sel.source_type_id as u16));
                    self.fuel_types.insert(sel.fuel_type_id as u16);
                }
            }
            ModelCombination::Nonroad => {}
        }
        if matches!(mc, ModelCombination::Nonroad | ModelCombination::Both) {
            for sel in &self.run_spec.offroad_vehicle_selections {
                self.sectors.insert(sel.sector_id as u16);
                self.fuel_types.insert(sel.fuel_type_id as u16);
            }
        }
    }

    // ---- Pollutant/process membership queries -------------------------------

    /// Check whether a specific `(pollutant, process)` pair is selected.
    ///
    /// Ports the three-arg `doesHavePollutantAndProcess(Pollutant, EmissionProcess)`.
    /// Wildcard variants are exposed via the `_by_*` helpers below.
    #[must_use]
    pub fn does_have_pollutant_and_process(
        &self,
        pollutant_id: PollutantId,
        process_id: ProcessId,
    ) -> bool {
        let assoc = PollutantProcessAssociation {
            pollutant_id,
            process_id,
        };
        self.pollutant_process_associations.contains(&assoc)
    }

    /// Wildcard query — pass `None` for either component to match any value.
    /// Returns `false` if both are `None` (matches Java's behaviour).
    ///
    /// Java's `doesHavePollutantAndProcess(Pollutant, EmissionProcess)`
    /// additionally filters by `Model.ModelCombination` for NONROAD — the
    /// runtime checks `isAffectedByNonroad` on the per-pair metadata
    /// stored in the default DB. That metadata isn't in
    /// [`moves_data::PollutantProcessAssociation`] (Task 14 keeps the
    /// identity-only layer pure); when Task 50's data plane lands, this
    /// method gains the M2 filter. For now it matches the M1 / default
    /// path: any selected pair matches without further filtering.
    #[must_use]
    pub fn does_have_pollutant_or_process(
        &self,
        pollutant_id: Option<PollutantId>,
        process_id: Option<ProcessId>,
    ) -> bool {
        match (pollutant_id, process_id) {
            (Some(p), Some(pr)) => self.does_have_pollutant_and_process(p, pr),
            (None, Some(pr)) => self.target_processes.contains(&pr),
            (Some(p), None) => self
                .pollutant_process_associations
                .iter()
                .any(|a| a.pollutant_id == p),
            (None, None) => false,
        }
    }

    /// Name-keyed wildcard query — accepts the same `""`/`null` wildcard
    /// pattern as Java's `doesHavePollutantAndProcess(String, String)`.
    #[must_use]
    pub fn does_have_pollutant_and_process_by_name(
        &self,
        pollutant_name: &str,
        process_name: &str,
    ) -> bool {
        let pollutant = if pollutant_name.is_empty() {
            None
        } else {
            match Pollutant::find_by_name(pollutant_name) {
                Some(p) => Some(p.id),
                None => return false,
            }
        };
        let process = if process_name.is_empty() {
            None
        } else {
            match EmissionProcess::find_by_name(process_name) {
                Some(p) => Some(p.id),
                None => return false,
            }
        };
        self.does_have_pollutant_or_process(pollutant, process)
    }

    /// Require a `(pollutant, process)` pair, flagging it for downstream
    /// removal if it was not already present.
    ///
    /// Returns `true` if the pair was newly added, `false` if it was
    /// already in the set (or the names didn't resolve). Ports `require(...)`
    /// minus the worker-SQL string generation — see
    /// [`silently_required`](Self::silently_required).
    pub fn require(&mut self, pollutant_name: &str, process_name: &str) -> bool {
        let Some(pollutant) = Pollutant::find_by_name(pollutant_name) else {
            return false;
        };
        let Some(process) = EmissionProcess::find_by_name(process_name) else {
            return false;
        };
        if self.does_have_pollutant_and_process(pollutant.id, process.id) {
            return false;
        }
        let assoc = PollutantProcessAssociation {
            pollutant_id: pollutant.id,
            process_id: process.id,
        };
        self.target_pollutants.insert(pollutant.id);
        self.target_processes.insert(process.id);
        self.pollutant_process_associations.insert(assoc);
        self.target_pollutant_processes.insert(assoc.polproc_id());
        self.silently_required.insert(assoc);
        true
    }

    /// Apply the static refueling-process dependency closure.
    ///
    /// Ports `flagRequiredPollutantProcesses`. Iterates the canonical
    /// `(output_pollutant, output_process) → (required_pollutant,
    /// required_process)` rules to a fixed point, calling
    /// [`require`](Self::require) whenever an output pair is selected.
    /// Skips on NONROAD-only runs to match the Java early-return.
    pub fn flag_required_pollutant_processes(&mut self) {
        if ModelCombination::from_run_spec(&self.run_spec) == ModelCombination::Nonroad {
            return;
        }
        let needs = REFUELING_NEEDS;
        let mut keep_going = true;
        while keep_going {
            keep_going = false;
            for &(out_pol, out_proc, req_pol, req_proc) in needs {
                if self.does_have_pollutant_and_process_by_name(out_pol, out_proc)
                    && self.require(req_pol, req_proc)
                {
                    keep_going = true;
                }
            }
        }
    }

    // ---- Geographic-set projection -----------------------------------------

    /// Re-derive `states`, `counties`, `zones`, `links` from the current
    /// `execution_locations`. Call after Task 16's producer populates
    /// `execution_locations`; idempotent.
    ///
    /// Members whose granularity-id fields are `None` contribute nothing —
    /// the set entries the location producer yields will have all four ids
    /// `Some`, but the type permits coarser entries and we skip them safely.
    pub fn extract_location_details_from_execution_locations(&mut self) {
        self.states.clear();
        self.counties.clear();
        self.zones.clear();
        self.links.clear();
        for loc in &self.execution_locations {
            if let Some(id) = loc.state_id {
                self.states.insert(id);
            }
            if let Some(id) = loc.county_id {
                self.counties.insert(id);
            }
            if let Some(id) = loc.zone_id {
                self.zones.insert(id);
            }
            if let Some(id) = loc.link_id {
                self.links.insert(id);
            }
        }
    }

    /// The effective road types for the run — port of
    /// `ExecutionRunSpec.getRoadTypes()`.
    ///
    /// Normally this is just the RunSpec's `road_types`. The one exception
    /// is Off-Network Idle: when the run selects the Off-Network road type
    /// (`roadTypeID` 1) *and* the Running Exhaust process (`processID` 1)
    /// and is not Project domain, MOVES must iterate every onroad road
    /// type to compute ONI correctly, so the method returns the full
    /// onroad set — the five [`moves_data::RoadType`] entries, which are
    /// exactly the default-DB `roadtype` rows flagged `isAffectedByOnroad`
    /// that Java's `getAllRoadTypes()` query returns.
    ///
    /// Returned as road-type ids: the sole consumer,
    /// [`ExecutionLocationProducer`], needs ids alone.
    #[must_use]
    pub fn execution_road_types(&self) -> BTreeSet<u32> {
        let selected: BTreeSet<u32> = self
            .run_spec
            .road_types
            .iter()
            .map(|rt| rt.road_type_id)
            .collect();
        let contains_off_network = selected.contains(&1);
        let contains_running = self.target_processes.contains(&ProcessId(1));
        let is_project_domain = self.run_spec.domain == Some(ModelDomain::Project);
        if contains_off_network && contains_running && !is_project_domain {
            moves_data::RoadType::all()
                .map(|rt| u32::from(rt.id.0))
                .collect()
        } else {
            selected
        }
    }

    /// Expand the run's geographic selections into `execution_locations`,
    /// then re-derive the `states` / `counties` / `zones` / `links`
    /// projections.
    ///
    /// Ports the `ExecutionLocationProducer` invocation in
    /// `ExecutionRunSpec.initializeBeforeExecutionDatabase`: build an
    /// [`ExecutionLocationProducer`] from the RunSpec's geographic
    /// selections and [`execution_road_types`](Self::execution_road_types),
    /// run it against `geography`, store the result in
    /// `execution_locations`, then call
    /// [`extract_location_details_from_execution_locations`](Self::extract_location_details_from_execution_locations).
    ///
    /// `geography` is the run's `Link` ⋈ `County` data. Phase 2 callers
    /// build a [`GeographyTables`] from fixtures; Task 50's data plane
    /// builds it from the `Link` / `County` Parquet snapshots. The
    /// custom-domain (`genericCounty`) path is not wired — the RunSpec
    /// model does not carry that field yet (a Task 12 follow-up) — so the
    /// producer is always built with no custom-domain county.
    pub fn build_execution_locations(&mut self, geography: &GeographyTables) {
        let producer = ExecutionLocationProducer::new(
            self.run_spec.geographic_selections.clone(),
            self.execution_road_types(),
            None,
        );
        self.execution_locations = producer.build_execution_locations(geography);
        self.extract_location_details_from_execution_locations();
    }

    // ---- Forwarding getters ------------------------------------------------

    /// `targetRunSpec.geographicOutputDetail`.
    #[must_use]
    pub fn geographic_output_detail(&self) -> GeographicOutputDetail {
        self.run_spec.geographic_output_detail
    }

    /// `targetRunSpec.outputTimeStep`.
    #[must_use]
    pub fn output_timestep(&self) -> OutputTimestep {
        self.run_spec.output_timestep
    }

    /// `targetRunSpec.scale`.
    #[must_use]
    pub fn model_scale(&self) -> ModelScale {
        self.run_spec.scale
    }

    /// `targetRunSpec.domain`. `None` matches Java's null (no domain set).
    #[must_use]
    pub fn model_domain(&self) -> Option<ModelDomain> {
        self.run_spec.domain
    }

    /// Number of iterations to run. Always `1` unless the RunSpec asked for
    /// uncertainty estimation — then it's the number of simulations.
    #[must_use]
    pub fn how_many_iterations_will_be_performed(&self) -> u32 {
        if self.run_spec.uncertainty.enabled {
            self.run_spec.uncertainty.simulations.max(1)
        } else {
            1
        }
    }

    /// `targetRunSpec.uncertainty.enabled`.
    #[must_use]
    pub fn estimate_uncertainty(&self) -> bool {
        self.run_spec.uncertainty.enabled
    }

    /// `Models.evaluateModels(targetRunSpec.models)` — which engine
    /// combination drives this run.
    #[must_use]
    pub fn model_combination(&self) -> ModelCombination {
        ModelCombination::from_run_spec(&self.run_spec)
    }

    // ---- Class-name allow/save lists ---------------------------------------

    /// Whether a calculator class is permitted to execute.
    ///
    /// Ports `shouldExecute(Class)`. Java walks the superclass chain;
    /// Rust has no class hierarchy, so the check is an exact-name match
    /// against [`classes_not_to_execute`](Self::classes_not_to_execute).
    /// Callers that want hierarchical semantics list the ancestor names
    /// explicitly in the calculator's registration metadata.
    #[must_use]
    pub fn should_execute(&self, name: &str) -> bool {
        !self.classes_not_to_execute.contains(name)
    }

    /// Whether data for a calculator should be preserved past its
    /// iteration. Ports `shouldSaveData(String)`. Mirrors Java's behaviour
    /// for the worker-databases override: when
    /// [`should_keep_worker_databases`](Self::should_keep_worker_databases)
    /// is set, `EmissionCalculator` saves regardless.
    #[must_use]
    pub fn should_save_data(&self, name: &str) -> bool {
        if self.should_keep_worker_databases
            && name.eq_ignore_ascii_case("gov.epa.otaq.moves.master.framework.EmissionCalculator")
        {
            return true;
        }
        self.classes_to_save_data.contains(name)
    }

    // ---- Aggregation-tracking hooks ----------------------------------------

    /// Register that a pollutant produces records benefiting from
    /// post-run aggregation. Ports the static `pollutantNeedsAggregation`.
    pub fn pollutant_needs_aggregation(&mut self, pollutant: PollutantId) {
        self.pollutants_needing_aggregation.insert(pollutant);
    }

    /// Register that a `(pollutant, process)` pair produces records
    /// benefiting from post-run aggregation. Ports the static
    /// `pollutantProcessNeedsAggregation`.
    pub fn pollutant_process_needs_aggregation(&mut self, assoc: PollutantProcessAssociation) {
        self.pollutant_processes_needing_aggregation.insert(assoc);
    }

    /// Decide whether the post-run aggregation pass should run.
    ///
    /// Ports `shouldDoFinalAggregation`. Java has a catch-all
    /// "default to true" fallback — we keep it, since calculators do not
    /// universally register aggregation requirements and the cheap-aggregate
    /// path is the safer default.
    #[must_use]
    pub fn should_do_final_aggregation(&self) -> bool {
        // 1. Explicit registration: any aggregation-needing PPA selected?
        if self
            .pollutant_processes_needing_aggregation
            .iter()
            .any(|a| self.target_pollutant_processes.contains(&a.polproc_id()))
        {
            return true;
        }
        // 2. Any aggregation-needing pollutant selected?
        if self
            .pollutants_needing_aggregation
            .iter()
            .any(|p| self.target_pollutants.contains(p))
        {
            return true;
        }
        // 3. Preaggregation timestep differs from final timestep.
        if let Some(agg) = self.aggregate_by_timestep() {
            if agg != self.run_spec.output_timestep {
                return true;
            }
        }
        // 4. More processes selected than the output breakdown asks for.
        if self.target_processes.len() > 1 && !self.run_spec.output_breakdown.emission_process {
            return true;
        }
        // 5. More road types selected than the output breakdown asks for.
        if self.run_spec.road_types.len() > 1 && !self.run_spec.output_breakdown.road_type {
            return true;
        }
        // 6. Geographic aggregation is needed when the requested output
        //    detail is coarser than the iteration detail.
        if self.geographic_aggregation_is_needed() {
            return true;
        }
        // 7. Java fallback — default-true so that newly-added calculators
        //    that forget to register aggregation requirements still produce
        //    coherent output. Documented in the original.
        true
    }

    fn aggregate_by_timestep(&self) -> Option<OutputTimestep> {
        self.run_spec
            .timespan
            .aggregate_by
            .as_deref()
            .and_then(OutputTimestep::from_xml_value)
    }

    fn geographic_aggregation_is_needed(&self) -> bool {
        // Locations are sorted by (state, county, zone, link). Coarser-than-
        // link entries (any Option field None) contribute nothing — we
        // compare the populated tuple.
        let mut has_multiple_states = false;
        let mut state_has_multiple_counties = false;
        let mut county_has_multiple_zones = false;
        let mut prior: Option<ExecutionLocation> = None;
        for &loc in &self.execution_locations {
            if let Some(p) = prior {
                if loc.state_id != p.state_id {
                    has_multiple_states = true;
                } else if loc.county_id != p.county_id {
                    state_has_multiple_counties = true;
                } else if loc.zone_id != p.zone_id {
                    county_has_multiple_zones = true;
                }
            }
            prior = Some(loc);
        }
        match self.run_spec.geographic_output_detail {
            GeographicOutputDetail::Nation => {
                has_multiple_states || state_has_multiple_counties || county_has_multiple_zones
            }
            GeographicOutputDetail::State => {
                state_has_multiple_counties || county_has_multiple_zones
            }
            GeographicOutputDetail::County => county_has_multiple_zones,
            GeographicOutputDetail::Zone | GeographicOutputDetail::Link => false,
        }
    }

    // ---- Per-iteration framework hooks (skeletons) -------------------------

    /// One-time pre-iteration setup. Ports the framework-side parts of
    /// `initializeBeforeIteration`: applies the worker-databases override
    /// when retrofit handling is registered. Today only the flag toggle is
    /// ported; the SQL-string assembly Task 25's
    /// `AggregationSQLGenerator` does is left to that task.
    pub fn initialize_before_iteration(&mut self) {
        // Java's `OnRoadRetrofitStrategy.shouldExecute` test gates this. We
        // mirror it with the class-name lookup; calculators that want the
        // retrofit-preserve behaviour register the strategy class name.
        let retrofit_class = "gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.onroadretrofit.OnRoadRetrofitStrategy";
        if self.should_execute(retrofit_class) && self.should_save_data(retrofit_class) {
            self.should_keep_worker_databases = true;
        }
    }

    // ---- Test-friendly accessors -------------------------------------------

    /// True if the runspec selected any rate-of-progress calculation. Ports
    /// `hasRateOfProgress`. The RunSpec model doesn't yet carry the flag
    /// (Task 12 follow-up), so this returns `false` for now.
    #[must_use]
    pub fn has_rate_of_progress(&self) -> bool {
        false
    }

    /// Whether the runspec contains a Running Exhaust pollutant whose
    /// display group does not require distance — used by some calculators
    /// to decide whether a distance-only chain is selectable.
    ///
    /// Ports `doesHaveDistancePollutantAndProcess`. The
    /// `PollutantDisplayGroup` metadata is data-plane state (Task 50);
    /// until then this conservatively returns whether *any* Running
    /// Exhaust pair is selected.
    #[must_use]
    pub fn does_have_distance_pollutant_and_process(&self) -> bool {
        // Running Exhaust is process id 1.
        self.does_have_pollutant_or_process(None, Some(ProcessId(1)))
    }

    /// The `MOVESOutput` table name. Matches Java's static constant — the
    /// table name never varies per-run today.
    #[must_use]
    pub const fn emission_output_table() -> &'static str {
        "MOVESOutput"
    }

    /// The `MOVESActivityOutput` table name. Same rationale as
    /// [`emission_output_table`](Self::emission_output_table).
    #[must_use]
    pub const fn activity_output_table() -> &'static str {
        "MOVESActivityOutput"
    }

    /// Convenience: the RunSpec's pollutant-process associations as the
    /// runspec parser produced them (with names retained).
    #[must_use]
    pub fn run_spec_associations(&self) -> &[RunSpecPollutantProcess] {
        &self.run_spec.pollutant_process_associations
    }
}

/// Static refueling-process dependency closure. Direct port of Java's
/// `needs[]` array in `flagRequiredPollutantProcesses`. Each row is
/// `(output_pollutant, output_process, required_pollutant, required_process)`:
/// when the user selects `(output_pollutant, output_process)`, the runtime
/// adds `(required_pollutant, required_process)` for chain completeness and
/// flags the addition for output-side stripping via `silently_required`.
const REFUELING_NEEDS: &[(&str, &str, &str, &str)] = &[
    // Refueling Displacement Vapor Loss.
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Displacement Vapor Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    // Refueling Spillage Loss.
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Total Gaseous Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Non-Methane Hydrocarbons",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Non-Methane Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Total Organic Gases",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Running Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Start Exhaust",
    ),
    (
        "Volatile Organic Compounds",
        "Refueling Spillage Loss",
        "Total Energy Consumption",
        "Extended Idle Exhaust",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_location_producer::{CountyRow, LinkRow};
    use moves_runspec::{
        GeoKind, GeographicSelection, Model, OnroadVehicleSelection,
        PollutantProcessAssociation as RsppA, RoadType, Timespan,
    };

    /// Build a RunSpec with just the fields the tests below touch. Fields
    /// not set use [`Default`] — the RunSpec model derives `Default`.
    /// Onroad-by-default is the most common shape in the fixtures and what
    /// the `flag_required_pollutant_processes` closure expects.
    fn build_run_spec(populate: impl FnOnce(&mut RunSpec)) -> RunSpec {
        let mut spec = RunSpec {
            models: vec![Model::Onroad],
            ..Default::default()
        };
        populate(&mut spec);
        spec
    }

    #[test]
    fn new_starts_with_will_run_calculators_true() {
        let er = ExecutionRunSpec::new(RunSpec::default());
        assert!(er.will_run_calculators);
    }

    #[test]
    fn target_sets_populate_from_runspec_ppas() {
        // (Total Energy Consumption = 91, Running Exhaust = 1)
        // (Carbon Monoxide = 2, Start Exhaust = 2)
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![
                RsppA {
                    pollutant_id: 91,
                    pollutant_name: "Total Energy Consumption".into(),
                    process_id: 1,
                    process_name: "Running Exhaust".into(),
                },
                RsppA {
                    pollutant_id: 2,
                    pollutant_name: "Carbon Monoxide (CO)".into(),
                    process_id: 2,
                    process_name: "Start Exhaust".into(),
                },
            ];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.target_pollutants.len(), 2);
        assert!(er.target_pollutants.contains(&PollutantId(91)));
        assert!(er.target_pollutants.contains(&PollutantId(2)));
        assert_eq!(er.target_processes.len(), 2);
        assert!(er.target_processes.contains(&ProcessId(1)));
        assert!(er.target_processes.contains(&ProcessId(2)));
        // polProcessIds: 91*100+1=9101, 2*100+2=202.
        assert!(er.target_pollutant_processes.contains(&PolProcessId(9101)));
        assert!(er.target_pollutant_processes.contains(&PolProcessId(202)));
    }

    #[test]
    fn timespan_sets_derive_from_runspec() {
        let spec = build_run_spec(|s| {
            s.timespan = Timespan {
                years: vec![2020, 2025],
                months: vec![1, 7],
                days: vec![2, 5],
                begin_hour: Some(8),
                end_hour: Some(10),
                aggregate_by: Some("Day".into()),
            };
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.years, BTreeSet::from([2020, 2025]));
        assert_eq!(er.months, BTreeSet::from([1, 7]));
        assert_eq!(er.days, BTreeSet::from([2, 5]));
        assert_eq!(er.hours, BTreeSet::from([8, 9, 10]));
        // hour_days = {8*10+2, 8*10+5, 9*10+2, 9*10+5, 10*10+2, 10*10+5}
        //           = {82, 85, 92, 95, 102, 105}
        assert_eq!(er.hour_days, BTreeSet::from([82, 85, 92, 95, 102, 105]));
    }

    #[test]
    fn missing_begin_hour_yields_empty_hours() {
        let spec = build_run_spec(|s| {
            s.timespan = Timespan {
                years: vec![2020],
                months: vec![1],
                days: vec![2],
                begin_hour: None,
                end_hour: None,
                ..Default::default()
            };
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.hours.is_empty());
        assert!(er.hour_days.is_empty());
    }

    #[test]
    fn vehicle_selection_sets_populate() {
        let spec = build_run_spec(|s| {
            s.onroad_vehicle_selections = vec![
                OnroadVehicleSelection {
                    fuel_type_id: 1,
                    fuel_type_name: "Gasoline".into(),
                    source_type_id: 21,
                    source_type_name: "Passenger Car".into(),
                },
                OnroadVehicleSelection {
                    fuel_type_id: 2,
                    fuel_type_name: "Diesel Fuel".into(),
                    source_type_id: 21,
                    source_type_name: "Passenger Car".into(),
                },
            ];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.source_types, BTreeSet::from([SourceTypeId(21)]));
        assert_eq!(er.fuel_types, BTreeSet::from([1u16, 2]));
        assert!(er.sectors.is_empty());
    }

    #[test]
    fn does_have_pollutant_and_process_full_match() {
        // (Total Energy Consumption = 91, Running Exhaust = 1)
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.does_have_pollutant_and_process(PollutantId(91), ProcessId(1)));
        // Same process, different pollutant — should miss the explicit match.
        assert!(!er.does_have_pollutant_and_process(PollutantId(2), ProcessId(1)));
    }

    #[test]
    fn does_have_pollutant_or_process_wildcards() {
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        // Process wildcard: query "anything with process 1" → true.
        assert!(er.does_have_pollutant_or_process(None, Some(ProcessId(1))));
        // Pollutant wildcard: query "anything with pollutant 91" → true.
        assert!(er.does_have_pollutant_or_process(Some(PollutantId(91)), None));
        // Both None → false (matches Java).
        assert!(!er.does_have_pollutant_or_process(None, None));
        // Unknown process → false.
        assert!(!er.does_have_pollutant_or_process(None, Some(ProcessId(99))));
    }

    #[test]
    fn does_have_pollutant_and_process_by_name_resolves_via_data_layer() {
        // Verify the name path goes through moves-data's Pollutant /
        // EmissionProcess lookups, not a parallel registry.
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.does_have_pollutant_and_process_by_name(
            "Total Energy Consumption",
            "Running Exhaust",
        ));
        // Empty name → wildcard.
        assert!(er.does_have_pollutant_and_process_by_name("", "Running Exhaust"));
        // Unknown name → false.
        assert!(!er.does_have_pollutant_and_process_by_name("Bogus Pollutant", "Running Exhaust"));
    }

    #[test]
    fn require_returns_true_when_adding_new_pair() {
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        let added = er.require("Total Energy Consumption", "Running Exhaust");
        assert!(added);
        assert!(er.does_have_pollutant_and_process(PollutantId(91), ProcessId(1)));
        assert_eq!(er.silently_required.len(), 1);
    }

    #[test]
    fn require_returns_false_when_pair_already_present() {
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let mut er = ExecutionRunSpec::new(spec);
        let added = er.require("Total Energy Consumption", "Running Exhaust");
        assert!(!added);
        // Silently-required set stays empty when the pair was user-added.
        assert!(er.silently_required.is_empty());
    }

    #[test]
    fn require_returns_false_for_unknown_names() {
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        assert!(!er.require("Bogus Pollutant", "Running Exhaust"));
        assert!(!er.require("Total Energy Consumption", "Bogus Process"));
    }

    #[test]
    fn flag_required_pollutant_processes_pulls_in_refueling_dependencies() {
        // Refueling Displacement Vapor Loss for THC should pull in
        // Total Energy Consumption for Running/Start/Extended Idle Exhaust.
        // THC = pollutant id 1, RDVL = process id 18.
        // Total Energy Consumption = 91; Running=1, Start=2, ExtIdle=90.
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 1,
                pollutant_name: "Total Gaseous Hydrocarbons".into(),
                process_id: 18,
                process_name: "Refueling Displacement Vapor Loss".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        // The user's pair stays.
        assert!(er.does_have_pollutant_and_process(PollutantId(1), ProcessId(18)));
        // All three energy-consumption requireds got added.
        assert!(er.does_have_pollutant_and_process(PollutantId(91), ProcessId(1)));
        assert!(er.does_have_pollutant_and_process(PollutantId(91), ProcessId(2)));
        assert!(er.does_have_pollutant_and_process(PollutantId(91), ProcessId(90)));
        // And they're flagged silently-required.
        assert_eq!(er.silently_required.len(), 3);
    }

    #[test]
    fn flag_required_is_skipped_on_nonroad_only_runs() {
        let mut spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 1,
                pollutant_name: "Total Gaseous Hydrocarbons".into(),
                process_id: 18,
                process_name: "Refueling Displacement Vapor Loss".into(),
            }];
        });
        spec.models = vec![Model::Nonroad];
        let er = ExecutionRunSpec::new(spec);
        // Nonroad-only path: no refueling closure runs.
        assert!(er.silently_required.is_empty());
        // The user-selected pair is still present.
        assert!(er.does_have_pollutant_and_process(PollutantId(1), ProcessId(18)));
        // But no Total Energy Consumption pulled in.
        assert!(!er.does_have_pollutant_and_process(PollutantId(91), ProcessId(1)));
    }

    #[test]
    fn extract_location_details_projects_from_locations() {
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        // Two links in (state=24, county=24001, zone=240010) and one in a
        // different state — should produce 2 states, 2 counties, 2 zones,
        // 3 links.
        er.execution_locations
            .insert(ExecutionLocation::link(24, 24001, 240010, 2400100));
        er.execution_locations
            .insert(ExecutionLocation::link(24, 24001, 240010, 2400101));
        er.execution_locations
            .insert(ExecutionLocation::link(51, 51001, 510010, 5100100));
        er.extract_location_details_from_execution_locations();
        assert_eq!(er.states, BTreeSet::from([24, 51]));
        assert_eq!(er.counties, BTreeSet::from([24001, 51001]));
        assert_eq!(er.zones, BTreeSet::from([240010, 510010]));
        assert_eq!(er.links, BTreeSet::from([2400100, 2400101, 5100100]));
    }

    #[test]
    fn extract_location_details_skips_none_fields() {
        // A coarser-than-link location (only state_id set) contributes
        // its state but nothing else — defensive coverage for the
        // Option-typed fields.
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        er.execution_locations.insert(ExecutionLocation::state(99));
        er.execution_locations
            .insert(ExecutionLocation::link(24, 24001, 240010, 2400100));
        er.extract_location_details_from_execution_locations();
        assert_eq!(er.states, BTreeSet::from([24, 99]));
        assert_eq!(er.counties, BTreeSet::from([24001]));
        assert_eq!(er.zones, BTreeSet::from([240010]));
        assert_eq!(er.links, BTreeSet::from([2400100]));
    }

    #[test]
    fn should_execute_respects_classes_not_to_execute() {
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        assert!(er.should_execute("SomeCalculator"));
        er.classes_not_to_execute
            .insert("SomeCalculator".to_string());
        assert!(!er.should_execute("SomeCalculator"));
        assert!(er.should_execute("OtherCalculator"));
    }

    #[test]
    fn should_save_data_honors_worker_db_override() {
        let mut er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        // Worker-databases off by default.
        assert!(!er.should_save_data("gov.epa.otaq.moves.master.framework.EmissionCalculator"));
        // Toggle the retrofit-driven flag: EmissionCalculator now saves.
        er.should_keep_worker_databases = true;
        assert!(er.should_save_data("gov.epa.otaq.moves.master.framework.EmissionCalculator"));
        // Other classes still gated by the explicit list.
        assert!(!er.should_save_data("SomeOtherCalculator"));
        er.classes_to_save_data
            .insert("SomeOtherCalculator".to_string());
        assert!(er.should_save_data("SomeOtherCalculator"));
    }

    #[test]
    fn uncertainty_iteration_count_follows_runspec_flag() {
        let mut spec = build_run_spec(|_| {});
        spec.uncertainty.enabled = false;
        spec.uncertainty.simulations = 5;
        let er = ExecutionRunSpec::new(spec.clone());
        assert_eq!(er.how_many_iterations_will_be_performed(), 1);
        assert!(!er.estimate_uncertainty());

        spec.uncertainty.enabled = true;
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.how_many_iterations_will_be_performed(), 5);
        assert!(er.estimate_uncertainty());
    }

    #[test]
    fn should_do_final_aggregation_triggers_on_aggregate_by_mismatch() {
        let spec = build_run_spec(|s| {
            s.output_timestep = OutputTimestep::Year;
            s.timespan = Timespan {
                aggregate_by: Some("Hour".into()),
                ..Default::default()
            };
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn should_do_final_aggregation_triggers_on_multiple_processes_without_breakdown() {
        let spec = build_run_spec(|s| {
            s.output_breakdown.emission_process = false;
            s.pollutant_process_associations = vec![
                RsppA {
                    pollutant_id: 91,
                    pollutant_name: "Total Energy Consumption".into(),
                    process_id: 1,
                    process_name: "Running Exhaust".into(),
                },
                RsppA {
                    pollutant_id: 91,
                    pollutant_name: "Total Energy Consumption".into(),
                    process_id: 2,
                    process_name: "Start Exhaust".into(),
                },
            ];
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn should_do_final_aggregation_triggers_on_multiple_road_types_without_breakdown() {
        let spec = build_run_spec(|s| {
            s.output_breakdown.road_type = false;
            s.road_types = vec![
                RoadType {
                    road_type_id: 2,
                    road_type_name: "Rural Restricted Access".into(),
                    model_combination: None,
                },
                RoadType {
                    road_type_id: 4,
                    road_type_name: "Urban Restricted Access".into(),
                    model_combination: None,
                },
            ];
        });
        let er = ExecutionRunSpec::new(spec);
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn should_do_final_aggregation_default_true_on_empty_runspec() {
        // Java's documented catch-all: in the absence of any signal,
        // aggregation runs by default. Empty runspec hits the fallback.
        let er = ExecutionRunSpec::new(build_run_spec(|_| {}));
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn pollutant_processes_needing_aggregation_drives_decision() {
        // Register a pair as aggregation-needing, then add it to the run.
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let mut er = ExecutionRunSpec::new(spec);
        er.pollutant_process_needs_aggregation(PollutantProcessAssociation {
            pollutant_id: PollutantId(91),
            process_id: ProcessId(1),
        });
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn pollutants_needing_aggregation_drives_decision() {
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let mut er = ExecutionRunSpec::new(spec);
        er.pollutant_needs_aggregation(PollutantId(91));
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn geographic_aggregation_check_at_nation_detail() {
        // Two states in the locations + Nation output → aggregation needed.
        let mut er = ExecutionRunSpec::new(build_run_spec(|s| {
            s.geographic_output_detail = GeographicOutputDetail::Nation;
        }));
        er.execution_locations
            .insert(ExecutionLocation::link(24, 1, 1, 1));
        er.execution_locations
            .insert(ExecutionLocation::link(51, 1, 1, 1));
        assert!(er.should_do_final_aggregation());
    }

    #[test]
    fn model_combination_from_runspec() {
        let mut spec = build_run_spec(|_| {});
        spec.models = vec![Model::Onroad];
        assert_eq!(
            ModelCombination::from_run_spec(&spec),
            ModelCombination::Onroad
        );

        spec.models = vec![Model::Nonroad];
        assert_eq!(
            ModelCombination::from_run_spec(&spec),
            ModelCombination::Nonroad
        );

        spec.models = vec![Model::Onroad, Model::Nonroad];
        assert_eq!(
            ModelCombination::from_run_spec(&spec),
            ModelCombination::Both
        );

        spec.models = vec![];
        // Empty defaults to Onroad (matches Java's evaluateModels default).
        assert_eq!(
            ModelCombination::from_run_spec(&spec),
            ModelCombination::Onroad
        );
    }

    #[test]
    fn nonroad_run_uses_offroad_selections_for_fuel_and_sector() {
        use moves_runspec::OffroadVehicleSelection;
        let mut spec = build_run_spec(|s| {
            s.offroad_vehicle_selections = vec![OffroadVehicleSelection {
                fuel_type_id: 2,
                fuel_type_name: "Diesel Fuel".into(),
                sector_id: 7,
                sector_name: "Construction Equipment".into(),
            }];
        });
        spec.models = vec![Model::Nonroad];
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.sectors, BTreeSet::from([7u16]));
        assert_eq!(er.fuel_types, BTreeSet::from([2u16]));
        // Source types stay empty for NONROAD.
        assert!(er.source_types.is_empty());
    }

    #[test]
    fn hour_day_id_uses_canonical_formula() {
        // hour 1, day 2 → 12. hour 24, day 5 → 245.
        assert_eq!(ExecutionRunSpec::hour_day_id(1, 2), 12);
        assert_eq!(ExecutionRunSpec::hour_day_id(24, 5), 245);
    }

    #[test]
    fn output_table_names_are_static_constants() {
        assert_eq!(ExecutionRunSpec::emission_output_table(), "MOVESOutput");
        assert_eq!(
            ExecutionRunSpec::activity_output_table(),
            "MOVESActivityOutput"
        );
    }

    #[test]
    fn run_spec_associations_returns_runspec_view() {
        let spec = build_run_spec(|s| {
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        let assocs = er.run_spec_associations();
        assert_eq!(assocs.len(), 1);
        assert_eq!(assocs[0].pollutant_name, "Total Energy Consumption");
        assert_eq!(assocs[0].process_name, "Running Exhaust");
    }

    // ---- Task 16: road types + location iterator ---------------------------

    /// Build a [`RoadType`] selection with just the id set.
    fn road_type(id: u32) -> RoadType {
        RoadType {
            road_type_id: id,
            road_type_name: String::new(),
            model_combination: None,
        }
    }

    #[test]
    fn execution_road_types_returns_runspec_selection() {
        // No Off-Network road type, so no Off-Network-Idle expansion: the
        // method returns exactly the RunSpec's road types.
        let spec = build_run_spec(|s| {
            s.road_types = vec![road_type(2), road_type(4)];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.execution_road_types(), BTreeSet::from([2, 4]));
    }

    #[test]
    fn execution_road_types_expands_for_off_network_idle() {
        // Off-Network road type (1) + Running Exhaust process (1), not
        // Project domain: every onroad road type (1–5) is required.
        let spec = build_run_spec(|s| {
            s.road_types = vec![road_type(1)];
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.execution_road_types(), BTreeSet::from([1, 2, 3, 4, 5]));
    }

    #[test]
    fn execution_road_types_no_expansion_on_project_domain() {
        // Project domain does not calculate ONI, so even with Off-Network
        // + Running selected the road-type set is left as-is.
        let spec = build_run_spec(|s| {
            s.domain = Some(ModelDomain::Project);
            s.road_types = vec![road_type(1)];
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 91,
                pollutant_name: "Total Energy Consumption".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.execution_road_types(), BTreeSet::from([1]));
    }

    #[test]
    fn execution_road_types_no_expansion_without_running_process() {
        // Off-Network road type selected but no Running Exhaust process:
        // ONI is not triggered, so no expansion.
        let spec = build_run_spec(|s| {
            s.road_types = vec![road_type(1)];
            s.pollutant_process_associations = vec![RsppA {
                pollutant_id: 2,
                pollutant_name: "Carbon Monoxide (CO)".into(),
                process_id: 2,
                process_name: "Start Exhaust".into(),
            }];
        });
        let er = ExecutionRunSpec::new(spec);
        assert_eq!(er.execution_road_types(), BTreeSet::from([1]));
    }

    #[test]
    fn build_execution_locations_populates_locations_and_projections() {
        // A county selection over a two-link county: the producer fills
        // `execution_locations` and the run re-derives the per-component
        // geographic sets.
        let spec = build_run_spec(|s| {
            s.geographic_selections = vec![GeographicSelection {
                kind: GeoKind::County,
                key: 24001,
                description: String::new(),
            }];
            s.road_types = vec![road_type(2), road_type(3)];
        });
        let mut er = ExecutionRunSpec::new(spec);
        let geo = GeographyTables::new(
            vec![
                LinkRow {
                    state_id: 24,
                    county_id: 24001,
                    zone_id: 240010,
                    link_id: 2400100,
                    road_type_id: 2,
                },
                LinkRow {
                    state_id: 24,
                    county_id: 24001,
                    zone_id: 240011,
                    link_id: 2400110,
                    road_type_id: 3,
                },
                // A link in a different county — excluded by the selection.
                LinkRow {
                    state_id: 51,
                    county_id: 51001,
                    zone_id: 510010,
                    link_id: 5100100,
                    road_type_id: 2,
                },
            ],
            vec![CountyRow {
                state_id: 24,
                county_id: 24001,
            }],
        );
        er.build_execution_locations(&geo);
        assert_eq!(
            er.execution_locations.iter().copied().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 240010, 2400100),
                ExecutionLocation::link(24, 24001, 240011, 2400110),
            ]
        );
        // `extract_location_details_from_execution_locations` ran.
        assert_eq!(er.states, BTreeSet::from([24]));
        assert_eq!(er.counties, BTreeSet::from([24001]));
        assert_eq!(er.zones, BTreeSet::from([240010, 240011]));
        assert_eq!(er.links, BTreeSet::from([2400100, 2400110]));
    }
}
