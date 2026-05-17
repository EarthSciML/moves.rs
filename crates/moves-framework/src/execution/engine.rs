//! `MOVESEngine` — the entry point that ties the framework together.
//!
//! Ports `gov.epa.otaq.moves.master.framework.MOVESEngine` (Task 27). The
//! Java original is a ~2k-line GUI/console singleton that owns a worker
//! thread pool, MariaDB connection management, distributed-bundle handoff,
//! and a heartbeat thread. The migration plan strips all of that: the Rust
//! port has no master/worker split, no MariaDB, and no filesystem-mediated
//! intermediate state, so the engine collapses to the orchestration spine.
//!
//! # The pipeline
//!
//! [`MOVESEngine::run`] walks the steps the migration plan lists for
//! Task 27 — *load RunSpec → instantiate ExecutionRunSpec → register
//! calculators → build CalculatorContext → run MasterLoop → finalize
//! OutputProcessor*:
//!
//! 1. **Instantiate [`ExecutionRunSpec`]** (Task 15) from the parsed
//!    [`RunSpec`] — the runtime view: target pollutants / processes,
//!    timespan sets, vehicle selections.
//! 2. **Plan the calculator graph** — [`CalculatorRegistry`] (Task 19)
//!    filters the chain DAG to the RunSpec's `(pollutant, process)`
//!    selections and topologically orders the result.
//! 3. **Chunk it** — [`chunk_chains`] (Task 27's executor) splits the
//!    planned modules into independent calculator chains.
//! 4. **Run** — one [`MasterLoop`] (Task 20) per chunk, dispatched through
//!    the [`BoundedExecutor`] so at most `--max-parallel-chunks` chains run
//!    concurrently. Each chunk instantiates its calculators / generators,
//!    wraps them in a `MasterLoopable` adapter, and subscribes them to its
//!    loop.
//! 5. **Finalize** — build the `MOVESRun` metadata record and hand it to
//!    [`OutputProcessor`] (Tasks 26 / 89), which writes `MOVESRun.parquet`.
//!
//! # Phase 2 status
//!
//! No Phase 3 calculators exist yet, so a registry typically has no
//! factories registered: every planned module is reported in
//! [`EngineOutcome::modules_unimplemented`] and the per-chunk
//! [`MasterLoop`]s run with no subscribers. That is the expected Phase 2
//! state — the engine machinery is complete and exercised; Task 28's
//! end-to-end test and the Phase 3 calculators fill in the bodies.
//!
//! Geographic iteration is similarly thin: [`ExecutionRunSpec`] leaves
//! [`execution_locations`](ExecutionRunSpec::execution_locations) empty
//! until Task 16's location producer and the Task 50 data plane land. A
//! caller (or a test) that populates it through
//! [`MOVESEngine::execution_run_spec_mut`] drives the full
//! `state → county → zone → link` nest immediately.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use moves_data::output_schema::{MovesRunRecord, OutputTable};
use moves_data::{PollutantId, ProcessId};
use moves_runspec::model::Model;
use moves_runspec::RunSpec;
use sha2::{Digest, Sha256};

use super::execution_db::ExecutionTime;
use super::execution_runspec::ExecutionRunSpec;
use super::executor::{chunk_chains, BoundedExecutor, Chunk};
use crate::aggregation::OutputProcessor;
use crate::calculator::{
    Calculator, CalculatorContext, CalculatorRegistry, CalculatorSubscription, Generator,
};
use crate::error::Result;
use crate::masterloop::{
    MasterLoop, MasterLoopContext, MasterLoopable, MasterLoopableSubscription,
};

/// Tunable inputs for one [`MOVESEngine`] run that do not come from the
/// [`RunSpec`] itself.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Directory the [`OutputProcessor`] writes into. Created if absent.
    pub output_root: PathBuf,
    /// Maximum number of calculator chains run concurrently — the
    /// `--max-parallel-chunks` lever. `0` selects the host's available
    /// parallelism. See [`crate::execution::executor`] for the memory model.
    pub max_parallel_chunks: usize,
    /// RunSpec file name recorded in the `MOVESRun` metadata row. `None`
    /// leaves the column null (e.g. a RunSpec built in memory).
    pub run_spec_file_name: Option<String>,
    /// Run timestamp recorded in the `MOVESRun` metadata row. Left to the
    /// caller — rather than stamped from the wall clock — so a run is
    /// reproducible and its `MOVESRun.parquet` is byte-stable for the
    /// snapshot-determinism contract. `None` leaves the column null.
    pub run_date_time: Option<String>,
}

impl EngineConfig {
    /// Config rooted at `output_root` with host-chosen parallelism and no
    /// metadata overrides.
    #[must_use]
    pub fn new(output_root: impl Into<PathBuf>) -> Self {
        Self {
            output_root: output_root.into(),
            max_parallel_chunks: 0,
            run_spec_file_name: None,
            run_date_time: None,
        }
    }
}

/// Summary of one [`MOVESEngine::run`] — what was planned, what executed,
/// and where the output landed.
#[derive(Debug, Clone)]
pub struct EngineOutcome {
    /// Number of MasterLoop iterations performed (one unless the RunSpec
    /// asked for uncertainty estimation).
    pub iterations: u32,
    /// Every calculator-graph module relevant to the RunSpec, in
    /// topological order.
    pub modules_planned: Vec<String>,
    /// The planned modules split into independent calculator chains.
    pub chunks: Vec<Chunk>,
    /// Planned modules that had a registered factory and were therefore
    /// instantiated and subscribed to a MasterLoop.
    pub modules_executed: Vec<String>,
    /// Planned modules with no registered factory — not yet ported.
    /// Expected to be the whole plan until Phase 3 lands calculators.
    pub modules_unimplemented: Vec<String>,
    /// The resolved parallelism limit (`config.max_parallel_chunks`, with
    /// `0` expanded to the host's available parallelism).
    pub max_parallel_chunks: usize,
    /// Directory the output was written to.
    pub output_root: PathBuf,
    /// Path of the `MOVESRun.parquet` metadata file.
    pub run_record_path: PathBuf,
}

impl EngineOutcome {
    /// Number of independent calculator chains the run was split into.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Whether every planned module had a registered factory — i.e. the
    /// calculator graph is fully ported. `false` throughout Phase 2.
    #[must_use]
    pub fn is_fully_implemented(&self) -> bool {
        self.modules_unimplemented.is_empty()
    }
}

/// A calculator or generator instance — the two module kinds the engine
/// instantiates from registry factories and runs through a MasterLoop.
#[derive(Debug)]
enum ModuleInstance {
    /// An emission [`Calculator`].
    Calculator(Box<dyn Calculator>),
    /// A [`Generator`].
    Generator(Box<dyn Generator>),
}

impl ModuleInstance {
    /// MasterLoop subscriptions the module declares.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        match self {
            ModuleInstance::Calculator(c) => c.subscriptions(),
            ModuleInstance::Generator(g) => g.subscriptions(),
        }
    }

    /// Run the module against `ctx`. The [`crate::CalculatorOutput`] is
    /// discarded — Task 50's data plane wires it into the scratch
    /// namespace; Phase 2 calculators return an empty output anyway.
    fn execute(&self, ctx: &CalculatorContext) -> Result<()> {
        match self {
            ModuleInstance::Calculator(c) => c.execute(ctx).map(|_| ()),
            ModuleInstance::Generator(g) => g.execute(ctx).map(|_| ()),
        }
    }
}

/// Adapter bridging a [`Calculator`] / [`Generator`] onto the
/// [`MasterLoopable`] trait the [`MasterLoop`] dispatches.
///
/// `calculator.rs` anticipates exactly this type: the calculator traits
/// deliberately do not inherit [`MasterLoopable`], because a calculator's
/// `execute` needs a [`CalculatorContext`] that the master-loop callback
/// does not carry. The adapter closes that gap — on each loop callback it
/// materialises a fresh [`CalculatorContext`] from the loop's
/// [`IterationPosition`](crate::IterationPosition) and invokes the module.
///
/// One adapter is created per [`CalculatorSubscription`]; the underlying
/// module instance is shared between them through the `Arc`. Each adapter
/// carries the [`ProcessId`] its subscription targets and no-ops when the
/// loop is iterating a different process — the master loop fires every
/// subscription at a matching granularity regardless of process, so the
/// per-process gate has to live here.
#[derive(Debug)]
struct CalculatorMasterLoopable {
    /// The shared calculator / generator instance.
    module: Arc<ModuleInstance>,
    /// Process the originating subscription is registered for. The adapter
    /// fires only while the loop iterates this process.
    gate_process: ProcessId,
}

impl MasterLoopable for CalculatorMasterLoopable {
    fn execute_at_granularity(&self, context: &MasterLoopContext) -> Result<()> {
        // Gate on process: the master loop calls every granularity-matching
        // subscription, but a calculator subscription is scoped to one
        // process.
        if context.position.process_id != Some(self.gate_process) {
            return Ok(());
        }
        let ctx = CalculatorContext::with_position(context.position);
        self.module.execute(&ctx)
    }
}

/// The MOVES execution engine — owns a run's [`ExecutionRunSpec`], its
/// [`CalculatorRegistry`], and the [`EngineConfig`], and drives the run.
///
/// See the [module docs](self) for the pipeline and Phase 2 caveats.
#[derive(Debug)]
pub struct MOVESEngine {
    /// Runtime view of the RunSpec being executed (Task 15).
    execution: ExecutionRunSpec,
    /// Calculator-graph DAG plus factory bindings (Task 19).
    registry: CalculatorRegistry,
    /// Output directory and parallelism tuning.
    config: EngineConfig,
}

impl MOVESEngine {
    /// Build an engine for `run_spec`, deriving its [`ExecutionRunSpec`].
    ///
    /// The `registry` carries the calculator-graph DAG and whatever Phase 3
    /// factory bindings have been registered; `config` supplies the output
    /// directory and parallelism limit.
    #[must_use]
    pub fn new(run_spec: RunSpec, registry: CalculatorRegistry, config: EngineConfig) -> Self {
        Self {
            execution: ExecutionRunSpec::new(run_spec),
            registry,
            config,
        }
    }

    /// The runtime view of the RunSpec.
    #[must_use]
    pub fn execution_run_spec(&self) -> &ExecutionRunSpec {
        &self.execution
    }

    /// Mutable access to the runtime view — chiefly so callers can populate
    /// [`execution_locations`](ExecutionRunSpec::execution_locations)
    /// before [`run`](Self::run) (Task 16's location producer, or a test).
    pub fn execution_run_spec_mut(&mut self) -> &mut ExecutionRunSpec {
        &mut self.execution
    }

    /// The calculator registry.
    #[must_use]
    pub fn registry(&self) -> &CalculatorRegistry {
        &self.registry
    }

    /// The engine configuration.
    #[must_use]
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// The RunSpec's `(pollutant, process)` selections, as the
    /// [`CalculatorRegistry`] filter expects them.
    fn selections(&self) -> Vec<(PollutantId, ProcessId)> {
        self.execution
            .pollutant_process_associations
            .iter()
            .map(|assoc| (assoc.pollutant_id, assoc.process_id))
            .collect()
    }

    /// Every calculator-graph module relevant to this run, topologically
    /// ordered (upstream producers before downstream consumers).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::CyclicChain`] if the filtered chain DAG has
    /// a cycle.
    pub fn planned_modules(&self) -> Result<Vec<String>> {
        self.registry
            .execution_order_for_runspec(&self.selections())
    }

    /// The planned modules split into independent calculator chains —
    /// what the [`BoundedExecutor`] dispatches.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::Error::CyclicChain`] from planning / chunking.
    pub fn planned_chunks(&self) -> Result<Vec<Chunk>> {
        let modules = self.planned_modules()?;
        let refs: Vec<&str> = modules.iter().map(String::as_str).collect();
        chunk_chains(&self.registry, &refs)
    }

    /// Execute the run: plan and chunk the calculator graph, drive one
    /// [`MasterLoop`] per chunk through the [`BoundedExecutor`], and write
    /// the `MOVESRun` metadata via the [`OutputProcessor`].
    ///
    /// # Errors
    ///
    /// * [`crate::Error::CyclicChain`] — the calculator chain DAG has a
    ///   cycle.
    /// * [`crate::Error::ThreadPool`] — the bounded-concurrency pool failed
    ///   to build.
    /// * [`crate::Error::Io`] / [`crate::Error::Arrow`] /
    ///   [`crate::Error::Parquet`] — writing `MOVESRun.parquet` failed.
    /// * Any error a calculator's `execute` surfaces during the run.
    pub fn run(&self) -> Result<EngineOutcome> {
        let modules_planned = self.planned_modules()?;
        let module_refs: Vec<&str> = modules_planned.iter().map(String::as_str).collect();
        let chunks = chunk_chains(&self.registry, &module_refs)?;

        // Shared, immutable per-run iteration plan handed to every chunk's
        // MasterLoop. `locations` is empty until Task 16 / the data plane
        // populate `execution_locations`; `times` is the timespan product.
        let iterations = self.execution.how_many_iterations_will_be_performed();
        let processes: Vec<ProcessId> = self.execution.target_processes.iter().copied().collect();
        let locations: Vec<_> = self.execution.execution_locations.iter().copied().collect();
        let times = build_times(&self.execution);

        let executor = BoundedExecutor::new(self.config.max_parallel_chunks)?;
        let registry = &self.registry;
        executor.execute(&chunks, |chunk| {
            let mut master = MasterLoop::new();
            master.iterations = iterations;
            master.processes = processes.clone();
            master.locations = locations.clone();
            master.times = times.clone();
            for name in chunk.modules() {
                let Some(instance) = instantiate(registry, name) else {
                    continue;
                };
                let module = Arc::new(instance);
                // One MasterLoop subscription per declared calculator
                // subscription, all sharing the single module instance.
                for sub in module.subscriptions() {
                    let adapter = Arc::new(CalculatorMasterLoopable {
                        module: Arc::clone(&module),
                        gate_process: sub.process_id,
                    });
                    master.subscribe(MasterLoopableSubscription::new(
                        sub.granularity,
                        sub.priority.value(),
                        adapter,
                    ));
                }
            }
            master.run()
        })?;

        // Partition the plan into executed (had a factory) and not-yet-ported.
        let mut modules_executed = Vec::new();
        let mut modules_unimplemented = Vec::new();
        for name in &modules_planned {
            if self.registry.has_factory(name) {
                modules_executed.push(name.clone());
            } else {
                modules_unimplemented.push(name.clone());
            }
        }

        // Finalize: write the MOVESRun metadata row.
        let run_record = self.build_run_record();
        OutputProcessor::new(&self.config.output_root, &run_record)?;
        let run_record_path = self
            .config
            .output_root
            .join(OutputProcessor::partition_path(
                OutputTable::Run,
                None,
                None,
            ));

        Ok(EngineOutcome {
            iterations,
            modules_planned,
            chunks,
            modules_executed,
            modules_unimplemented,
            max_parallel_chunks: executor.limit(),
            output_root: self.config.output_root.clone(),
            run_record_path,
        })
    }

    /// Build the single `MOVESRun` metadata row from the RunSpec's output
    /// settings — the Rust analogue of `MOVESEngine.createOutputRunRecord`,
    /// minus the MariaDB `INSERT`.
    ///
    /// Wall-clock fields (`run_date_time`, `minutes_duration`) are not
    /// stamped here: `run_date_time` comes from the [`EngineConfig`] so a
    /// run stays reproducible, and `minutes_duration` is left null.
    fn build_run_record(&self) -> MovesRunRecord {
        let run_spec = &self.execution.run_spec;
        let version = format!("moves-framework {}", env!("CARGO_PKG_VERSION"));
        MovesRunRecord {
            moves_run_id: 1,
            output_time_period: Some(run_spec.output_timestep.xml_value().to_string()),
            time_units: Some(run_spec.output_factors.time.units.xml_value().to_string()),
            distance_units: Some(
                run_spec
                    .output_factors
                    .distance
                    .units
                    .xml_value()
                    .to_string(),
            ),
            mass_units: Some(run_spec.output_factors.mass.units.xml_value().to_string()),
            energy_units: Some(
                run_spec
                    .output_factors
                    .mass
                    .energy_units
                    .xml_value()
                    .to_string(),
            ),
            run_spec_file_name: self.config.run_spec_file_name.clone(),
            run_spec_description: run_spec.description.clone(),
            run_spec_file_date_time: None,
            run_date_time: self.config.run_date_time.clone(),
            scale: Some(run_spec.scale.xml_value().to_string()),
            minutes_duration: None,
            default_database_used: None,
            master_version: Some(version.clone()),
            master_computer_id: None,
            master_id_number: None,
            domain: run_spec.domain.map(|d| d.xml_value().to_string()),
            domain_county_id: None,
            domain_county_name: None,
            domain_database_server: None,
            domain_database_name: None,
            expected_done_files: None,
            retrieved_done_files: None,
            models: Some(models_label(&run_spec.models)),
            run_hash: run_hash(run_spec),
            calculator_version: version,
        }
    }
}

/// Instantiate the named module from the registry, trying the calculator
/// factory first then the generator factory. `None` if neither is
/// registered (the Phase 2 default).
fn instantiate(registry: &CalculatorRegistry, name: &str) -> Option<ModuleInstance> {
    if let Some(calc) = registry.instantiate_calculator(name) {
        return Some(ModuleInstance::Calculator(calc));
    }
    registry
        .instantiate_generator(name)
        .map(ModuleInstance::Generator)
}

/// Build the fully-specified `(year, month, day, hour)` tuples the
/// MasterLoop's time nest iterates — the Cartesian product of the
/// [`ExecutionRunSpec`] timespan sets.
///
/// Out-of-range ids (a year past `u16`, a month/day/hour past `u8`) are
/// skipped; every value the RunSpec parser produces is well within range.
fn build_times(execution: &ExecutionRunSpec) -> Vec<ExecutionTime> {
    let mut times = Vec::new();
    for &year in &execution.years {
        let Ok(year) = u16::try_from(year) else {
            continue;
        };
        for &month in &execution.months {
            let Ok(month) = u8::try_from(month) else {
                continue;
            };
            for &day in &execution.days {
                let Ok(day) = u8::try_from(day) else {
                    continue;
                };
                for &hour in &execution.hours {
                    let Ok(hour) = u8::try_from(hour) else {
                        continue;
                    };
                    times.push(ExecutionTime::hour(year, month, day, hour));
                }
            }
        }
    }
    times
}

/// Render the RunSpec's model selections as the lowercase, comma-joined
/// label the `MOVESRun.models` column carries. An empty selection defaults
/// to `"onroad"`, matching the Java `MOVESRun` column default.
fn models_label(models: &[Model]) -> String {
    if models.is_empty() {
        return "onroad".to_string();
    }
    models
        .iter()
        .map(|m| match m {
            Model::Onroad => "onroad",
            Model::Nonroad => "nonroad",
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Hex SHA-256 of the canonical run inputs — the JSON serialization of the
/// [`RunSpec`]. Deterministic for a given RunSpec; stamped into the
/// `MOVESRun.runHash` column.
fn run_hash(run_spec: &RunSpec) -> String {
    let json =
        serde_json::to_vec(run_spec).expect("RunSpec is plain data and always serializes to JSON");
    let digest = Sha256::digest(&json);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Path of the `MOVESRun.parquet` file under an output root — exposed so a
/// CLI (Task 28) can predict the layout without re-deriving it.
#[must_use]
pub fn run_record_path(output_root: &Path) -> PathBuf {
    output_root.join(OutputProcessor::partition_path(
        OutputTable::Run,
        None,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculator::CalculatorOutput;
    use moves_calculator_info::{build_dag, parse_calculator_info_str, Granularity, Priority};
    use moves_runspec::model::{ModelScale, PollutantProcessAssociation, Timespan};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    // ---- Test doubles ------------------------------------------------------

    /// Calculator that bumps a counter on every `execute`. The counter is a
    /// `&'static AtomicUsize` so the type works both for direct
    /// construction and behind a non-capturing factory `fn`.
    #[derive(Debug)]
    struct StubCalc {
        name: &'static str,
        subs: Vec<CalculatorSubscription>,
        runs: &'static AtomicUsize,
    }

    impl Calculator for StubCalc {
        fn name(&self) -> &'static str {
            self.name
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            &self.subs
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &[]
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(CalculatorOutput::empty())
        }
    }

    /// Generator that bumps a counter on every `execute`.
    #[derive(Debug)]
    struct StubGen {
        name: &'static str,
        subs: Vec<CalculatorSubscription>,
        runs: &'static AtomicUsize,
    }

    impl Generator for StubGen {
        fn name(&self) -> &'static str {
            self.name
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            &self.subs
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(CalculatorOutput::empty())
        }
    }

    /// One PROCESS-granularity subscription for Running Exhaust (process 1)
    /// — fires once per process iteration, independent of the location nest.
    fn process_subs() -> Vec<CalculatorSubscription> {
        vec![CalculatorSubscription::new(
            ProcessId(1),
            Granularity::Process,
            Priority::parse("EMISSION_CALCULATOR").unwrap(),
        )]
    }

    // Per-test execution counters. Each static is referenced by exactly one
    // test (through exactly one factory), so the parallel test runner never
    // races on them.
    static ENGINE_RUN_CALC: AtomicUsize = AtomicUsize::new(0);
    static PARTITION_RUN_CALC: AtomicUsize = AtomicUsize::new(0);
    static ADAPTER_GATE_CALC: AtomicUsize = AtomicUsize::new(0);
    static ADAPTER_GEN: AtomicUsize = AtomicUsize::new(0);

    /// Factory for the `run_executes_a_registered_calculator` test.
    fn base_rate_factory() -> Box<dyn Calculator> {
        Box::new(StubCalc {
            name: "BaseRateCalculator",
            subs: process_subs(),
            runs: &ENGINE_RUN_CALC,
        })
    }

    /// Factory for the `run_partitions_modules_by_registered_factory` test —
    /// a separate counter keeps it from racing the test above.
    fn partition_factory() -> Box<dyn Calculator> {
        Box::new(StubCalc {
            name: "BaseRateCalculator",
            subs: process_subs(),
            runs: &PARTITION_RUN_CALC,
        })
    }

    // ---- RunSpec / registry fixtures --------------------------------------

    /// RunSpec selecting CO (pollutant 2) for Running Exhaust (process 1),
    /// a single county-scale July weekday hour.
    fn sample_runspec() -> RunSpec {
        RunSpec {
            models: vec![Model::Onroad],
            scale: ModelScale::Inventory,
            pollutant_process_associations: vec![PollutantProcessAssociation {
                pollutant_id: 2,
                pollutant_name: "CO".to_string(),
                process_id: 1,
                process_name: "Running Exhaust".to_string(),
            }],
            timespan: Timespan {
                years: vec![2020],
                months: vec![7],
                days: vec![5],
                begin_hour: Some(8),
                end_hour: Some(8),
                aggregate_by: None,
            },
            ..RunSpec::default()
        }
    }

    /// Registry over a one-calculator DAG: BaseRateCalculator, registered
    /// for (CO, Running Exhaust), subscribing at PROCESS granularity.
    fn single_calc_registry() -> CalculatorRegistry {
        let info = parse_calculator_info_str(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\n",
            Path::new("test"),
        )
        .unwrap();
        CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
    }

    /// Registry over a two-module chain: UpstreamGen → BaseRateCalculator.
    fn chained_registry() -> CalculatorRegistry {
        let info = parse_calculator_info_str(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\n\
             Subscribe\tUpstreamGen\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tBaseRateCalculator\tUpstreamGen\n",
            Path::new("test"),
        )
        .unwrap();
        CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
    }

    fn config(root: &Path) -> EngineConfig {
        EngineConfig::new(root)
    }

    // ---- ExecutionRunSpec wiring ------------------------------------------

    #[test]
    fn new_derives_the_execution_run_spec() {
        let engine = MOVESEngine::new(
            sample_runspec(),
            single_calc_registry(),
            config(Path::new("/tmp/unused")),
        );
        let exec = engine.execution_run_spec();
        assert!(exec.target_pollutants.contains(&PollutantId(2)));
        assert!(exec.target_processes.contains(&ProcessId(1)));
        assert!(exec.years.contains(&2020));
        assert!(exec.hours.contains(&8));
    }

    // ---- Planning ----------------------------------------------------------

    #[test]
    fn planned_modules_filters_to_the_runspec_selection() {
        let engine = MOVESEngine::new(
            sample_runspec(),
            single_calc_registry(),
            config(Path::new("/tmp/unused")),
        );
        assert_eq!(
            engine.planned_modules().unwrap(),
            vec!["BaseRateCalculator".to_string()]
        );
    }

    #[test]
    fn planned_modules_is_empty_when_no_pair_matches() {
        // RunSpec selects (pollutant 99, process 99) — nothing in the DAG.
        let mut spec = sample_runspec();
        spec.pollutant_process_associations = vec![PollutantProcessAssociation {
            pollutant_id: 99,
            pollutant_name: "X".to_string(),
            process_id: 99,
            process_name: "Y".to_string(),
        }];
        let engine = MOVESEngine::new(spec, single_calc_registry(), config(Path::new("/tmp/x")));
        assert!(engine.planned_modules().unwrap().is_empty());
    }

    #[test]
    fn planned_chunks_match_chunk_chains_over_the_plan() {
        let engine = MOVESEngine::new(
            sample_runspec(),
            chained_registry(),
            config(Path::new("/tmp/unused")),
        );
        let chunks = engine.planned_chunks().unwrap();
        // UpstreamGen → BaseRateCalculator is one connected chain.
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].modules(), ["UpstreamGen", "BaseRateCalculator"]);
    }

    // ---- run() -------------------------------------------------------------

    #[test]
    fn run_writes_the_moves_run_metadata_file() {
        let dir = tempdir().unwrap();
        let engine = MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()));
        let outcome = engine.run().unwrap();
        assert_eq!(outcome.run_record_path, dir.path().join("MOVESRun.parquet"));
        assert!(outcome.run_record_path.is_file());
        assert!(outcome.run_record_path.metadata().unwrap().len() > 0);
    }

    #[test]
    fn run_marks_every_module_unimplemented_without_factories() {
        let dir = tempdir().unwrap();
        let engine = MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()));
        let outcome = engine.run().unwrap();
        assert_eq!(
            outcome.modules_planned,
            vec!["BaseRateCalculator".to_string()]
        );
        assert!(outcome.modules_executed.is_empty());
        assert_eq!(
            outcome.modules_unimplemented,
            vec!["BaseRateCalculator".to_string()]
        );
        assert!(!outcome.is_fully_implemented());
        assert_eq!(outcome.iterations, 1);
        assert_eq!(outcome.chunk_count(), 1);
    }

    #[test]
    fn run_partitions_modules_by_registered_factory() {
        let dir = tempdir().unwrap();
        let mut registry = chained_registry();
        registry
            .register_calculator("BaseRateCalculator", partition_factory)
            .unwrap();
        let engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        let outcome = engine.run().unwrap();
        assert_eq!(
            outcome.modules_executed,
            vec!["BaseRateCalculator".to_string()]
        );
        assert_eq!(
            outcome.modules_unimplemented,
            vec!["UpstreamGen".to_string()]
        );
    }

    #[test]
    fn run_executes_a_registered_calculator() {
        ENGINE_RUN_CALC.store(0, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut registry = single_calc_registry();
        registry
            .register_calculator("BaseRateCalculator", base_rate_factory)
            .unwrap();
        let engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        engine.run().unwrap();
        // One iteration × one process × one PROCESS-granularity subscription.
        assert_eq!(ENGINE_RUN_CALC.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_resolves_the_parallelism_limit() {
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.max_parallel_chunks = 3;
        let engine = MOVESEngine::new(sample_runspec(), single_calc_registry(), cfg);
        assert_eq!(engine.run().unwrap().max_parallel_chunks, 3);

        let dir2 = tempdir().unwrap();
        let engine2 = MOVESEngine::new(
            sample_runspec(),
            single_calc_registry(),
            config(dir2.path()),
        );
        // `0` expands to the host's available parallelism.
        let outcome = engine2.run().unwrap();
        assert!(outcome.max_parallel_chunks >= 1);
    }

    #[test]
    fn run_drives_more_iterations_when_uncertainty_is_enabled() {
        let dir = tempdir().unwrap();
        let mut spec = sample_runspec();
        spec.uncertainty.enabled = true;
        spec.uncertainty.simulations = 4;
        let engine = MOVESEngine::new(spec, single_calc_registry(), config(dir.path()));
        assert_eq!(engine.run().unwrap().iterations, 4);
    }

    // ---- Run-record construction ------------------------------------------

    #[test]
    fn build_run_record_maps_runspec_fields() {
        let mut cfg = config(Path::new("/tmp/unused"));
        cfg.run_spec_file_name = Some("sample.mrs".to_string());
        cfg.run_date_time = Some("2026-05-17T00:00:00".to_string());
        let mut spec = sample_runspec();
        spec.description = Some("engine fixture".to_string());
        let engine = MOVESEngine::new(spec, single_calc_registry(), cfg);

        let record = engine.build_run_record();
        assert_eq!(record.moves_run_id, 1);
        assert_eq!(record.scale.as_deref(), Some("Inv"));
        assert_eq!(record.output_time_period.as_deref(), Some("Hour"));
        assert_eq!(record.models.as_deref(), Some("onroad"));
        assert_eq!(record.run_spec_file_name.as_deref(), Some("sample.mrs"));
        assert_eq!(record.run_date_time.as_deref(), Some("2026-05-17T00:00:00"));
        assert_eq!(
            record.run_spec_description.as_deref(),
            Some("engine fixture")
        );
        // SHA-256 hex is 64 lowercase hex chars.
        assert_eq!(record.run_hash.len(), 64);
        assert!(record.run_hash.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn run_hash_is_deterministic_and_input_sensitive() {
        let base = sample_runspec();
        let same = sample_runspec();
        assert_eq!(run_hash(&base), run_hash(&same));

        let mut different = sample_runspec();
        different.description = Some("changed".to_string());
        assert_ne!(run_hash(&base), run_hash(&different));
    }

    #[test]
    fn models_label_joins_and_defaults() {
        assert_eq!(models_label(&[]), "onroad");
        assert_eq!(models_label(&[Model::Onroad]), "onroad");
        assert_eq!(models_label(&[Model::Nonroad]), "nonroad");
        assert_eq!(
            models_label(&[Model::Onroad, Model::Nonroad]),
            "onroad,nonroad"
        );
    }

    #[test]
    fn build_times_is_the_timespan_product() {
        let mut spec = sample_runspec();
        spec.timespan = Timespan {
            years: vec![2020, 2021],
            months: vec![1, 7],
            days: vec![5],
            begin_hour: Some(7),
            end_hour: Some(8),
            aggregate_by: None,
        };
        let exec = ExecutionRunSpec::new(spec);
        // 2 years × 2 months × 1 day × 2 hours = 8 fully-specified tuples.
        let times = build_times(&exec);
        assert_eq!(times.len(), 8);
        assert!(times.iter().all(|t| {
            t.year.is_some() && t.month.is_some() && t.day_id.is_some() && t.hour.is_some()
        }));
    }

    // ---- Adapter -----------------------------------------------------------

    #[test]
    fn adapter_runs_the_module_only_for_its_gated_process() {
        ADAPTER_GATE_CALC.store(0, Ordering::SeqCst);
        let calc = StubCalc {
            name: "BaseRateCalculator",
            subs: process_subs(),
            runs: &ADAPTER_GATE_CALC,
        };
        let adapter = CalculatorMasterLoopable {
            module: Arc::new(ModuleInstance::Calculator(Box::new(calc))),
            gate_process: ProcessId(1),
        };

        let mut ctx = MasterLoopContext::default();
        // Matching process: the module runs.
        ctx.position.process_id = Some(ProcessId(1));
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(ADAPTER_GATE_CALC.load(Ordering::SeqCst), 1);

        // Different process: gated out, no run.
        ctx.position.process_id = Some(ProcessId(2));
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(ADAPTER_GATE_CALC.load(Ordering::SeqCst), 1);

        // No process set (e.g. start-of-run): gated out.
        ctx.position.process_id = None;
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(ADAPTER_GATE_CALC.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn adapter_runs_generators_too() {
        ADAPTER_GEN.store(0, Ordering::SeqCst);
        let generator = StubGen {
            name: "UpstreamGen",
            subs: process_subs(),
            runs: &ADAPTER_GEN,
        };
        let adapter = CalculatorMasterLoopable {
            module: Arc::new(ModuleInstance::Generator(Box::new(generator))),
            gate_process: ProcessId(1),
        };
        let mut ctx = MasterLoopContext::default();
        ctx.position.process_id = Some(ProcessId(1));
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(ADAPTER_GEN.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_record_path_helper_matches_the_output_layout() {
        let root = Path::new("/runs/output");
        assert_eq!(run_record_path(root), root.join("MOVESRun.parquet"));
    }
}
