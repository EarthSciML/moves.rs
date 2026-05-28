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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::data::InMemoryStore;

use moves_data::output_schema::{EmissionRecord, MovesRunRecord, OutputTable};
use moves_data::{PollutantId, ProcessId};
use moves_runspec::model::Model;
use moves_runspec::RunSpec;
use polars::prelude::DataFrame;
use sha2::{Digest, Sha256};

use super::execution_db::ExecutionTime;
use super::execution_runspec::ExecutionRunSpec;
use super::executor::{chunk_chains, BoundedExecutor, Chunk};
use crate::aggregation::{
    emission_aggregation, AggregationInputs, OutputProcessor, StreamingEmissionAgg, UnitScaling,
};
use crate::calculator::{
    Calculator, CalculatorContext, CalculatorRegistry, CalculatorSubscription, Generator,
};
use crate::control_strategy::{ControlStrategyRegistry, InternalControlStrategy};
use crate::error::Result;
use crate::masterloop::{
    MasterLoop, MasterLoopContext, MasterLoopable, MasterLoopableSubscription,
};

/// Tunable inputs for one [`MOVESEngine`] run that do not come from the
/// [`RunSpec`] itself.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Directory the [`OutputProcessor`] writes into. Created if absent.
    /// Ignored when [`collect_output_in_memory`](Self::collect_output_in_memory)
    /// is `true`.
    pub output_root: PathBuf,
    /// Maximum number of calculator chains run concurrently — the
    /// `--max-parallel-chunks` lever. `0` selects the host's available
    /// parallelism. See [`crate::execution::executor`] for the memory model.
    ///
    /// The WASM build (Task 132) defaults this to 1 because `wasm32-unknown-unknown`
    /// has no threads until Task 134 enables the threads proposal.
    pub max_parallel_chunks: usize,
    /// RunSpec file name recorded in the `MOVESRun` metadata row. `None`
    /// leaves the column null (e.g. a RunSpec built in memory).
    pub run_spec_file_name: Option<String>,
    /// Run timestamp recorded in the `MOVESRun` metadata row. Left to the
    /// caller — rather than stamped from the wall clock — so a run is
    /// reproducible and its `MOVESRun.parquet` is byte-stable for the
    /// snapshot-determinism contract. `None` leaves the column null.
    pub run_date_time: Option<String>,
    /// When `true`, output Parquet bytes are collected in memory and returned
    /// in [`EngineOutcome::output_bytes`] rather than written to
    /// [`output_root`](Self::output_root). Used by the `wasm32` build (Task 132)
    /// where no real filesystem is available.
    pub collect_output_in_memory: bool,
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
            collect_output_in_memory: false,
        }
    }
}

/// Summary of one [`MOVESEngine::run`] — what was planned, what executed,
/// and where the output landed.
///
/// The timing fields (`wall_time`, `planning_time`, `execution_time`) are
/// zero-valued until Task 75 wires them in, but are present from that task
/// onward so downstream tooling (the `moves run` summary, perf tests) can
/// read them without touching the engine innards.
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

    // --- Task 75: performance baseline fields ---
    /// Total wall time from the start of [`MOVESEngine::run`] to the return
    /// of the finalisation step, including output-file I/O.
    pub wall_time: Duration,
    /// Time spent planning (topological sort + chunking) before the
    /// executor starts.
    pub planning_time: Duration,
    /// Time spent running all MasterLoops through the [`BoundedExecutor`].
    /// Parallel chunks overlap, so this is wall time from executor dispatch
    /// to executor return — not the sum of per-chunk times.
    pub execution_time: Duration,
    /// Wall time for each calculator chain (chunk), in the same order as
    /// [`chunks`](Self::chunks). Each entry is the wall time for that chunk's
    /// `MasterLoop::run` call — measured within the parallel closure, so
    /// entries from concurrent chunks overlap in real time.
    pub chunk_wall_times: Vec<Duration>,
    /// Peak resident-set size in KiB at the end of the run, read from
    /// `/proc/self/status` (`VmHWM`). `None` on non-Linux hosts or when
    /// `/proc` is unavailable.
    pub peak_rss_kib: Option<u64>,

    /// Names of all control strategies that were instantiated and driven
    /// through `pre_run → per-iteration → post_run` for this run, in
    /// registration order. Empty when no strategies are registered.
    pub strategies_applied: Vec<String>,

    /// Output Parquet files collected in memory, populated when
    /// [`EngineConfig::collect_output_in_memory`] was `true`.
    /// Each entry is `(relative-path, bytes)` in the same layout that
    /// [`OutputProcessor`] would write to disk. Empty when filesystem
    /// output was used instead.
    pub output_bytes: Vec<(PathBuf, Vec<u8>)>,
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

    /// Run the module against `ctx`. Returns the calculator's emission
    /// DataFrame if any, or `None` for generators and empty outputs.
    ///
    /// Accepts `&mut CalculatorContext` so generators can write to
    /// `ctx.scratch_mut()`. Calculators receive a shared `&CalculatorContext`
    /// via the automatic coercion from `&mut`.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<Option<DataFrame>> {
        match self {
            ModuleInstance::Calculator(c) => Ok(c.execute(ctx)?.into_dataframe()),
            ModuleInstance::Generator(g) => {
                g.execute(ctx)?;
                Ok(None)
            }
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
/// updates the shared per-chunk [`CalculatorContext`] position and invokes
/// the module.
///
/// All adapters within the same chunk share one `Arc<Mutex<CalculatorContext>>`,
/// so generator writes to `ctx.scratch_mut()` are visible to downstream
/// calculators in the same chunk (which execute later in the sorted
/// subscription order). Different chunks hold different `Arc`s, giving
/// per-chunk scratch isolation.
///
/// One adapter is created per [`CalculatorSubscription`]; the underlying
/// module instance is shared between them through the `Arc`. Each adapter
/// carries the [`ProcessId`] its subscription targets and no-ops when the
/// loop is iterating a different process.
#[derive(Debug)]
struct CalculatorMasterLoopable {
    /// The shared calculator / generator instance.
    module: Arc<ModuleInstance>,
    /// Process the originating subscription is registered for. The adapter
    /// fires only while the loop iterates this process.
    gate_process: ProcessId,
    /// Per-chunk shared context. All adapters within one chunk share the
    /// same `Arc`; adapters in different chunks hold different `Arc`s.
    ctx: Arc<Mutex<CalculatorContext>>,
    /// Cross-chunk streaming aggregator: emission rows are folded into running
    /// group-by sums as they are produced, so peak memory is bounded by the
    /// number of distinct aggregation groups rather than the raw row count.
    streaming_agg: Arc<Mutex<StreamingEmissionAgg>>,
    /// Run hash stamped into every accumulated [`EmissionRecord`].
    run_hash: Arc<str>,
}

impl MasterLoopable for CalculatorMasterLoopable {
    fn execute_at_granularity(&self, context: &MasterLoopContext) -> Result<()> {
        // Gate on process: the master loop calls every granularity-matching
        // subscription, but a calculator subscription is scoped to one
        // process.
        if context.position.process_id != Some(self.gate_process) {
            return Ok(());
        }
        let df = {
            let mut ctx = self.ctx.lock().expect("CalculatorContext mutex poisoned");
            ctx.set_position(context.position);
            self.module.execute(&mut ctx)?
            // ctx lock released here — conversion and accumulator write happen outside
        };
        if let Some(df) = df {
            // Only accumulate emission output (has pollutantID); skip activity
            // output (e.g. MOVESWorkerActivityOutput from DistanceCalculator).
            if df.height() > 0 && df.column("pollutantID").is_ok() {
                let records = frame_to_emission_records(&df, &self.run_hash);
                // DataFrame is freed here before the accumulator lock is taken.
                drop(df);
                self.streaming_agg
                    .lock()
                    .expect("output accumulator poisoned")
                    .extend(&records, &UnitScaling)?;
            }
        }
        Ok(())
    }
}

/// Adapter bridging an [`InternalControlStrategy`] onto the [`MasterLoopable`]
/// trait. One adapter is created per [`StrategySubscription`]; the underlying
/// strategy instance is shared across all adapters (and across chunks) via
/// `Arc`. Each adapter carries the [`ProcessId`] its subscription targets and
/// no-ops when the loop is iterating a different process.
#[derive(Debug)]
struct StrategyMasterLoopable {
    strategy: Arc<dyn InternalControlStrategy>,
    gate_process: ProcessId,
}

impl MasterLoopable for StrategyMasterLoopable {
    fn execute_at_granularity(&self, context: &MasterLoopContext) -> Result<()> {
        if context.position.process_id != Some(self.gate_process) {
            return Ok(());
        }
        let ctx = CalculatorContext::with_position(context.position);
        self.strategy.execute(&ctx)
    }
}

/// The MOVES execution engine — owns a run's [`ExecutionRunSpec`], its
/// [`CalculatorRegistry`], its [`ControlStrategyRegistry`], and the
/// [`EngineConfig`], and drives the run.
///
/// See the [module docs](self) for the pipeline and Phase 2 caveats.
#[derive(Debug)]
pub struct MOVESEngine {
    /// Runtime view of the RunSpec being executed (Task 15).
    execution: ExecutionRunSpec,
    /// Calculator-graph DAG plus factory bindings (Task 19).
    registry: CalculatorRegistry,
    /// Control-strategy factories (Task 119). Empty by default.
    strategy_registry: ControlStrategyRegistry,
    /// Output directory and parallelism tuning.
    config: EngineConfig,
    /// Pre-loaded execution-database tables shared across all chunk contexts.
    /// Populated by [`with_slow_store`](Self::with_slow_store); empty by
    /// default (calculators receive an empty slow tier).
    slow_store: Arc<InMemoryStore>,
}

impl MOVESEngine {
    /// Build an engine for `run_spec`, deriving its [`ExecutionRunSpec`].
    ///
    /// The `registry` carries the calculator-graph DAG and whatever Phase 3
    /// factory bindings have been registered; `config` supplies the output
    /// directory and parallelism limit. Control strategies default to none;
    /// use [`with_strategy_registry`](Self::with_strategy_registry) to attach
    /// strategies.
    #[must_use]
    pub fn new(run_spec: RunSpec, registry: CalculatorRegistry, config: EngineConfig) -> Self {
        Self {
            execution: ExecutionRunSpec::new(run_spec),
            registry,
            strategy_registry: ControlStrategyRegistry::new(),
            config,
            slow_store: Arc::new(InMemoryStore::new()),
        }
    }

    /// Pre-load execution-database tables into every chunk's
    /// [`CalculatorContext`] slow tier.
    ///
    /// Call this after [`new`](Self::new) and before [`run`](Self::run) to
    /// give calculators access to the filtered default-DB / execution-DB
    /// tables they read via `ctx.tables()`. Without this call the slow tier
    /// is empty and any `iter_typed` call in a calculator will return a
    /// "table not found" error.
    #[must_use]
    pub fn with_slow_store(mut self, store: InMemoryStore) -> Self {
        self.slow_store = Arc::new(store);
        self
    }

    /// Attach a [`ControlStrategyRegistry`] to this engine. Strategies are
    /// instantiated and driven through `pre_run → per-iteration → post_run`
    /// on the next call to [`run`](Self::run).
    pub fn with_strategy_registry(mut self, sr: ControlStrategyRegistry) -> Self {
        self.strategy_registry = sr;
        self
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
    pub fn run(&mut self) -> Result<EngineOutcome> {
        let t_start = Instant::now();

        let t_plan_start = Instant::now();
        let modules_planned = self.planned_modules()?;
        let module_refs: Vec<&str> = modules_planned.iter().map(String::as_str).collect();
        let chunks = chunk_chains(&self.registry, &module_refs)?;
        let planning_time = t_plan_start.elapsed();

        // Shared, immutable per-run iteration plan handed to every chunk's
        // MasterLoop. `locations` is empty until Task 16 / the data plane
        // populate `execution_locations`; `times` is the timespan product.
        let iterations = self.execution.how_many_iterations_will_be_performed();
        let processes: Vec<ProcessId> = self.execution.target_processes.iter().copied().collect();
        let locations: Vec<_> = self.execution.execution_locations.iter().copied().collect();
        let times = build_times(&self.execution);

        // Instantiate control strategies and run their pre_run hooks before
        // the master loop starts. pre_run runs single-threaded here.
        let strategies: Vec<Arc<dyn InternalControlStrategy>> = self
            .strategy_registry
            .instantiate_all()
            .into_iter()
            .map(Arc::from)
            .collect();
        let strategies_applied: Vec<String> =
            strategies.iter().map(|s| s.name().to_string()).collect();
        for s in &strategies {
            s.pre_run(Arc::make_mut(&mut self.slow_store))?;
        }

        // Pre-build the run record — it depends only on the immutable RunSpec
        // and EngineConfig, so it is safe to construct before the executor runs.
        // The run_hash is also handed to each CalculatorMasterLoopable so that
        // DataFrames can be converted to EmissionRecords inline (and freed)
        // rather than buffered for a post-run bulk conversion.
        let run_record = self.build_run_record();
        let run_hash_str: Arc<str> = run_record.run_hash.clone().into();

        // Build the emission aggregation plan upfront so the streaming
        // accumulator can fold records directly into running group-by sums
        // during execution rather than buffering raw rows until run end.
        // Peak memory is bounded by N_distinct_groups, not N_raw_rows.
        let agg_inputs = aggregation_inputs_from_run_spec(&self.execution.run_spec);
        let emission_plan = emission_aggregation(&agg_inputs);
        let streaming_agg: Arc<Mutex<StreamingEmissionAgg>> =
            Arc::new(Mutex::new(StreamingEmissionAgg::new(emission_plan)?));
        let streaming_agg_ref = Arc::clone(&streaming_agg);

        let executor = BoundedExecutor::new(self.config.max_parallel_chunks)?;
        let registry = &self.registry;
        let chunk_slow = Arc::clone(&self.slow_store);
        // Per-chunk wall times collected from within the parallel closure.
        // Indexed to match the `chunks` slice order: `chunk_slot[i]` holds
        // the timing for `chunks[i]`. Using a `Mutex<Vec<Option<...>>>` lets
        // parallel closures write their slot without contending on others.
        let chunk_slot: Arc<Mutex<Vec<Option<Duration>>>> =
            Arc::new(Mutex::new(vec![None; chunks.len()]));
        let chunk_slot_ref = Arc::clone(&chunk_slot);
        let t_exec_start = Instant::now();
        executor.execute(&chunks, |chunk| {
            let t_chunk = Instant::now();
            let mut master = MasterLoop::new();
            master.iterations = iterations;
            master.processes = processes.clone();
            master.locations = locations.clone();
            master.times = times.clone();
            // Subscribe control strategies at INTERNAL_CONTROL_STRATEGY priority
            // (fires before generators and emission calculators). The same Arc
            // instance is shared across all chunks; execute() must be thread-safe.
            for strategy in &strategies {
                for sub in strategy.subscriptions() {
                    let adapter = Arc::new(StrategyMasterLoopable {
                        strategy: Arc::clone(strategy),
                        gate_process: sub.process_id,
                    });
                    master.subscribe(MasterLoopableSubscription::new(
                        sub.granularity,
                        sub.priority(),
                        adapter,
                    ));
                }
            }
            // One CalculatorContext per chunk: generators write scratch here,
            // calculators in the same chunk read it. Different chunks get
            // different Arcs, providing per-chunk scratch isolation.
            // The slow tier (execution-DB tables) is shared read-only across
            // all chunks via the same Arc.
            let chunk_ctx: Arc<Mutex<CalculatorContext>> = Arc::new(Mutex::new(
                CalculatorContext::with_slow(Arc::clone(&chunk_slow)),
            ));
            for name in chunk.modules() {
                let Some(instance) = instantiate(registry, name) else {
                    continue;
                };
                let module = Arc::new(instance);
                // One MasterLoop subscription per declared calculator
                // subscription, all sharing the single module instance and
                // the chunk's CalculatorContext.
                for sub in module.subscriptions() {
                    let adapter = Arc::new(CalculatorMasterLoopable {
                        module: Arc::clone(&module),
                        gate_process: sub.process_id,
                        ctx: Arc::clone(&chunk_ctx),
                        streaming_agg: Arc::clone(&streaming_agg_ref),
                        run_hash: Arc::clone(&run_hash_str),
                    });
                    master.subscribe(MasterLoopableSubscription::new(
                        sub.granularity,
                        sub.priority.value(),
                        adapter,
                    ));
                }
            }
            master.run()?;
            let elapsed = t_chunk.elapsed();
            // Find the slot that corresponds to this chunk by pointer identity.
            // Safe: `chunk` is a reference into the `chunks` slice that was
            // passed to `execute`, so `ptr::eq` finds the exact element.
            if let Ok(mut slots) = chunk_slot_ref.lock() {
                if let Some(idx) = chunks.iter().position(|c| std::ptr::eq(c, chunk)) {
                    slots[idx] = Some(elapsed);
                }
            }
            Ok(())
        })?;

        // post_run hooks run single-threaded after all chunks complete.
        let post_run_ctx = CalculatorContext::new();
        for s in &strategies {
            s.post_run(&post_run_ctx)?;
        }
        let execution_time = t_exec_start.elapsed();
        let chunk_wall_times: Vec<Duration> = chunk_slot
            .lock()
            .map(|slots| slots.iter().map(|opt| opt.unwrap_or_default()).collect())
            .unwrap_or_else(|_| vec![Duration::ZERO; chunks.len()]);

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

        // Finalize: drain the streaming aggregator (aggregation was applied
        // inline during execution) and write the rolled-up emission rows.
        // Drop the local ref clone first so Arc::try_unwrap sees count == 1.
        drop(streaming_agg_ref);
        let aggregated_records = Arc::try_unwrap(streaming_agg)
            .expect("streaming_agg still has owners after executor finished")
            .into_inner()
            .expect("streaming_agg mutex poisoned")
            .finalize();

        let (proc, output_bytes) = if self.config.collect_output_in_memory {
            let p = OutputProcessor::new_memory(&run_record)?;
            if !aggregated_records.is_empty() {
                p.write_emissions(&aggregated_records)?;
            }
            let bytes = p.take_memory_files().unwrap_or_default();
            (p, bytes)
        } else {
            let p = OutputProcessor::new(&self.config.output_root, &run_record)?;
            if !aggregated_records.is_empty() {
                p.write_emissions(&aggregated_records)?;
            }
            (p, Vec::new())
        };
        let run_record_path = proc.output_root().join(OutputProcessor::partition_path(
            OutputTable::Run,
            None,
            None,
        ));

        let wall_time = t_start.elapsed();
        let peak_rss_kib = read_peak_rss_kib();

        Ok(EngineOutcome {
            iterations,
            modules_planned,
            chunks,
            modules_executed,
            modules_unimplemented,
            max_parallel_chunks: executor.limit(),
            output_root: self.config.output_root.clone(),
            run_record_path,
            wall_time,
            planning_time,
            execution_time,
            chunk_wall_times,
            peak_rss_kib,
            strategies_applied,
            output_bytes,
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

/// Peak resident-set size in KiB, read from `/proc/self/status` (`VmHWM`).
///
/// `VmHWM` is a monotonic high-water mark: it reports the largest RSS the
/// process has ever had, not the current value, so it is safe to call at
/// the end of a run and still captures the peak from within the run.
/// Returns `None` on non-Linux hosts or when `/proc` is unavailable.
fn read_peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmHWM:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
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

/// Build an [`AggregationInputs`] from a [`RunSpec`].
fn aggregation_inputs_from_run_spec(run_spec: &RunSpec) -> AggregationInputs<'_> {
    AggregationInputs {
        timestep: run_spec.output_timestep,
        geographic_output_detail: run_spec.geographic_output_detail,
        scale: run_spec.scale,
        domain: run_spec.domain,
        models: &run_spec.models,
        breakdown: &run_spec.output_breakdown,
        output_population: false,
        reg_class_id: false,
        fuel_sub_type: false,
        eng_tech_id: false,
        sector: false,
    }
}

/// Convert one raw `MOVESWorkerOutput` [`DataFrame`] to [`EmissionRecord`] rows.
///
/// Each DataFrame column is read by name; missing columns yield `None` for
/// that field. All produced records carry `moves_run_id = 1` and the supplied
/// `run_hash`. Called inline from [`CalculatorMasterLoopable::execute_at_granularity`]
/// so the DataFrame can be freed immediately after conversion.
fn frame_to_emission_records(df: &DataFrame, run_hash: &str) -> Vec<EmissionRecord> {
    // Resolve column handles once per frame — O(columns) cost, no per-row Vec.
    // Missing or wrong-type columns become None; .get(i) does lazy per-element access.
    let year_ca = df.column("yearID").ok().and_then(|s| s.i32().ok());
    let month_ca = df.column("monthID").ok().and_then(|s| s.i32().ok());
    let day_ca = df.column("dayID").ok().and_then(|s| s.i32().ok());
    let hour_ca = df.column("hourID").ok().and_then(|s| s.i32().ok());
    let state_ca = df.column("stateID").ok().and_then(|s| s.i32().ok());
    let county_ca = df.column("countyID").ok().and_then(|s| s.i32().ok());
    let zone_ca = df.column("zoneID").ok().and_then(|s| s.i32().ok());
    let link_ca = df.column("linkID").ok().and_then(|s| s.i32().ok());
    let pollutant_ca = df.column("pollutantID").ok().and_then(|s| s.i32().ok());
    let process_ca = df.column("processID").ok().and_then(|s| s.i32().ok());
    let source_type_ca = df.column("sourceTypeID").ok().and_then(|s| s.i32().ok());
    let model_year_ca = df.column("modelYearID").ok().and_then(|s| s.i32().ok());
    let fuel_type_ca = df.column("fuelTypeID").ok().and_then(|s| s.i32().ok());
    let road_type_ca = df.column("roadTypeID").ok().and_then(|s| s.i32().ok());
    let emission_ca = df.column("emissionQuant").ok().and_then(|s| s.f64().ok());

    (0..df.height())
        .map(|i| EmissionRecord {
            moves_run_id: 1,
            iteration_id: None,
            year_id: year_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            month_id: month_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            day_id: day_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            hour_id: hour_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            state_id: state_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            county_id: county_ca.and_then(|c| c.get(i)),
            zone_id: zone_ca.and_then(|c| c.get(i)),
            link_id: link_ca.and_then(|c| c.get(i)),
            pollutant_id: pollutant_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            process_id: process_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            source_type_id: source_type_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            reg_class_id: None,
            fuel_type_id: fuel_type_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            fuel_sub_type_id: None,
            model_year_id: model_year_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            road_type_id: road_type_ca.and_then(|c| c.get(i)).map(|v| v as i16),
            scc: None,
            eng_tech_id: None,
            sector_id: None,
            hp_id: None,
            emission_quant: emission_ca.and_then(|c| c.get(i)),
            emission_rate: None,
            run_hash: run_hash.to_string(),
        })
        .collect()
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
    use crate::control_strategy::{
        ControlStrategyRegistry, InternalControlStrategy, StrategySubscription,
    };
    use crate::data::InMemoryStore;
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
        fn execute(&self, _ctx: &mut CalculatorContext) -> Result<CalculatorOutput> {
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
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()));
        let outcome = engine.run().unwrap();
        assert_eq!(outcome.run_record_path, dir.path().join("MOVESRun.parquet"));
        assert!(outcome.run_record_path.is_file());
        assert!(outcome.run_record_path.metadata().unwrap().len() > 0);
    }

    #[test]
    fn run_marks_every_module_unimplemented_without_factories() {
        let dir = tempdir().unwrap();
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()));
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
        let mut engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
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
        let mut engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        engine.run().unwrap();
        // One iteration × one process × one PROCESS-granularity subscription.
        assert_eq!(ENGINE_RUN_CALC.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_resolves_the_parallelism_limit() {
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.max_parallel_chunks = 3;
        let mut engine = MOVESEngine::new(sample_runspec(), single_calc_registry(), cfg);
        assert_eq!(engine.run().unwrap().max_parallel_chunks, 3);

        let dir2 = tempdir().unwrap();
        let mut engine2 = MOVESEngine::new(
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
        let mut engine = MOVESEngine::new(spec, single_calc_registry(), config(dir.path()));
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

    fn stub_chunk_ctx() -> Arc<Mutex<CalculatorContext>> {
        Arc::new(Mutex::new(CalculatorContext::new()))
    }

    fn stub_streaming_agg() -> Arc<Mutex<crate::aggregation::StreamingEmissionAgg>> {
        use crate::aggregation::{emission_aggregation, AggregationInputs, StreamingEmissionAgg};
        use moves_runspec::model::{
            GeographicOutputDetail, Model, ModelScale, OutputBreakdown, OutputTimestep,
        };
        let models = vec![Model::Onroad];
        let breakdown = OutputBreakdown::default();
        let agg_inputs = AggregationInputs {
            timestep: OutputTimestep::Hour,
            geographic_output_detail: GeographicOutputDetail::County,
            scale: ModelScale::Inventory,
            domain: None,
            models: &models,
            breakdown: &breakdown,
            output_population: false,
            reg_class_id: false,
            fuel_sub_type: false,
            eng_tech_id: false,
            sector: false,
        };
        let plan = emission_aggregation(&agg_inputs);
        Arc::new(Mutex::new(StreamingEmissionAgg::new(plan).unwrap()))
    }

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
            ctx: stub_chunk_ctx(),
            streaming_agg: stub_streaming_agg(),
            run_hash: Arc::from("test"),
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
            ctx: stub_chunk_ctx(),
            streaming_agg: stub_streaming_agg(),
            run_hash: Arc::from("test"),
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

    // ---- Control-strategy integration ------------------------------------
    //
    // Each test uses its own static counters and strategy type so the
    // parallel test runner cannot race on shared state — the same pattern
    // the calculator tests above use for ENGINE_RUN_CALC / PARTITION_RUN_CALC.

    // --- strategies_applied test ---
    #[derive(Debug)]
    struct NamedStrategy;
    impl InternalControlStrategy for NamedStrategy {
        fn name(&self) -> &'static str {
            "NamedStrategy"
        }
    }
    fn named_strategy_factory() -> Box<dyn InternalControlStrategy> {
        Box::new(NamedStrategy)
    }

    #[test]
    fn run_with_no_strategies_reports_empty_strategies_applied() {
        let dir = tempdir().unwrap();
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()));
        let outcome = engine.run().unwrap();
        assert!(outcome.strategies_applied.is_empty());
    }

    #[test]
    fn run_with_strategy_reports_name_in_strategies_applied() {
        let dir = tempdir().unwrap();
        let mut sr = ControlStrategyRegistry::new();
        sr.register(named_strategy_factory);
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()))
                .with_strategy_registry(sr);
        let outcome = engine.run().unwrap();
        assert_eq!(outcome.strategies_applied, vec!["NamedStrategy"]);
    }

    // --- pre_run / post_run fire once ---
    static LIFECYCLE_PRE: AtomicUsize = AtomicUsize::new(0);
    static LIFECYCLE_POST: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct LifecycleStrategy;
    impl InternalControlStrategy for LifecycleStrategy {
        fn name(&self) -> &'static str {
            "LifecycleStrategy"
        }
        fn pre_run(&self, _tables: &mut InMemoryStore) -> Result<()> {
            LIFECYCLE_PRE.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn post_run(&self, _ctx: &CalculatorContext) -> Result<()> {
            LIFECYCLE_POST.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn lifecycle_strategy_factory() -> Box<dyn InternalControlStrategy> {
        Box::new(LifecycleStrategy)
    }

    #[test]
    fn strategy_pre_and_post_run_fire_once() {
        LIFECYCLE_PRE.store(0, Ordering::SeqCst);
        LIFECYCLE_POST.store(0, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut sr = ControlStrategyRegistry::new();
        sr.register(lifecycle_strategy_factory);
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()))
                .with_strategy_registry(sr);
        engine.run().unwrap();
        assert_eq!(LIFECYCLE_PRE.load(Ordering::SeqCst), 1);
        assert_eq!(LIFECYCLE_POST.load(Ordering::SeqCst), 1);
    }

    // --- execute fires per-process-iteration ---
    static EXEC_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct ExecCountStrategy;

    static EXEC_COUNT_SUBS: &[StrategySubscription] = &[StrategySubscription {
        process_id: ProcessId(1),
        granularity: Granularity::Process,
        priority_offset: 0,
    }];

    impl InternalControlStrategy for ExecCountStrategy {
        fn name(&self) -> &'static str {
            "ExecCountStrategy"
        }
        fn subscriptions(&self) -> &[StrategySubscription] {
            EXEC_COUNT_SUBS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<()> {
            EXEC_COUNT.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn exec_count_factory() -> Box<dyn InternalControlStrategy> {
        Box::new(ExecCountStrategy)
    }

    #[test]
    fn strategy_execute_fires_per_process_iteration() {
        EXEC_COUNT.store(0, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut sr = ControlStrategyRegistry::new();
        sr.register(exec_count_factory);
        // sample_runspec selects one process (Running Exhaust, id 1). The
        // single_calc_registry produces one chunk, so ExecCountStrategy.execute
        // fires once: one process × one chunk.
        let mut engine =
            MOVESEngine::new(sample_runspec(), single_calc_registry(), config(dir.path()))
                .with_strategy_registry(sr);
        engine.run().unwrap();
        assert_eq!(EXEC_COUNT.load(Ordering::SeqCst), 1);
    }

    // --- adapter gate test ---
    static GATE_CTR: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct GatedStrategy;
    impl InternalControlStrategy for GatedStrategy {
        fn name(&self) -> &'static str {
            "GatedStrategy"
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<()> {
            GATE_CTR.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn strategy_adapter_gates_on_process_id() {
        // Direct unit-test of StrategyMasterLoopable: fires for the matching
        // process and no-ops for others.
        GATE_CTR.store(0, Ordering::SeqCst);
        let strategy: Arc<dyn InternalControlStrategy> = Arc::new(GatedStrategy);
        let adapter = StrategyMasterLoopable {
            strategy: Arc::clone(&strategy),
            gate_process: ProcessId(1),
        };
        let mut ctx = MasterLoopContext::default();
        // Matching process fires.
        ctx.position.process_id = Some(ProcessId(1));
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(GATE_CTR.load(Ordering::SeqCst), 1);
        // Different process: gated out.
        ctx.position.process_id = Some(ProcessId(2));
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(GATE_CTR.load(Ordering::SeqCst), 1);
        // No process set: gated out.
        ctx.position.process_id = None;
        adapter.execute_at_granularity(&ctx).unwrap();
        assert_eq!(GATE_CTR.load(Ordering::SeqCst), 1);
    }

    // ---- chunk_* tests: per-chunk scratch isolation and visibility ----------
    //
    // These tests verify:
    // 1. Generators write scratch; downstream calculators in the same chunk
    //    can read it (topo-order visibility).
    // 2. Separate chunks have disjoint scratch namespaces (isolation).
    //
    // Each test uses its own static AtomicBool flags and named `fn` factories
    // so the parallel test runner never races on shared state.

    use std::sync::atomic::AtomicBool;

    /// Generator that writes a one-row DataFrame under a fixed scratch key.
    #[derive(Debug)]
    struct ScratchWriterGen {
        name: &'static str,
        subs: Vec<CalculatorSubscription>,
        key: &'static str,
    }

    impl Generator for ScratchWriterGen {
        fn name(&self) -> &'static str {
            self.name
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            &self.subs
        }
        fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput> {
            use polars::prelude::{DataFrame, NamedFrom, Series};
            let s = Series::new("val".into(), [1i32]);
            let df = DataFrame::new(1, vec![s.into()]).unwrap();
            ctx.scratch_mut().insert(self.key, df);
            Ok(CalculatorOutput::empty())
        }
    }

    /// Calculator that records whether `key` is present in the chunk scratch.
    #[derive(Debug)]
    struct ScratchCheckCalc {
        name: &'static str,
        subs: Vec<CalculatorSubscription>,
        key: &'static str,
        found: &'static AtomicBool,
    }

    impl Calculator for ScratchCheckCalc {
        fn name(&self) -> &'static str {
            self.name
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            &self.subs
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &[]
        }
        fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput> {
            use crate::data::DataFrameStore;
            self.found.store(
                ctx.scratch().store.get(self.key).is_some(),
                Ordering::SeqCst,
            );
            Ok(CalculatorOutput::empty())
        }
    }

    fn gen_process_subs() -> Vec<CalculatorSubscription> {
        vec![CalculatorSubscription::new(
            ProcessId(1),
            Granularity::Process,
            Priority::parse("GENERATOR").unwrap(),
        )]
    }

    fn calc_process_subs() -> Vec<CalculatorSubscription> {
        vec![CalculatorSubscription::new(
            ProcessId(1),
            Granularity::Process,
            Priority::parse("EMISSION_CALCULATOR").unwrap(),
        )]
    }

    /// Build a single-chain registry: writer (generator) → checker (calculator).
    fn single_scratch_registry(writer: &'static str, checker: &'static str) -> CalculatorRegistry {
        let info_str = "Registration\tCO\t2\tRunning Exhaust\t1\t".to_string()
            + checker
            + "\nSubscribe\t"
            + checker
            + "\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\nSubscribe\t"
            + writer
            + "\tRunning Exhaust\t1\tPROCESS\tGENERATOR\nChain\t"
            + checker
            + "\t"
            + writer
            + "\n";
        let info = parse_calculator_info_str(&info_str, Path::new("test")).unwrap();
        CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
    }

    /// Build a two-chain registry: two independent (writer → checker) chains.
    fn two_chain_registry(
        writer_a: &'static str,
        checker_a: &'static str,
        writer_b: &'static str,
        checker_b: &'static str,
    ) -> CalculatorRegistry {
        let info_str = "Registration\tCO\t2\tRunning Exhaust\t1\t".to_string()
            + checker_a
            + "\nRegistration\tCO\t2\tRunning Exhaust\t1\t"
            + checker_b
            + "\nSubscribe\t"
            + checker_a
            + "\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\nSubscribe\t"
            + writer_a
            + "\tRunning Exhaust\t1\tPROCESS\tGENERATOR\nChain\t"
            + checker_a
            + "\t"
            + writer_a
            + "\nSubscribe\t"
            + checker_b
            + "\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\nSubscribe\t"
            + writer_b
            + "\tRunning Exhaust\t1\tPROCESS\tGENERATOR\nChain\t"
            + checker_b
            + "\t"
            + writer_b
            + "\n";
        let info = parse_calculator_info_str(&info_str, Path::new("test")).unwrap();
        CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
    }

    // -- chunk_scratch_visible_within_chunk_topo_order ----------------------

    static VISIBLE_FOUND: AtomicBool = AtomicBool::new(false);

    fn visible_writer_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "VisibleWriter",
            subs: gen_process_subs(),
            key: "visible_key",
        })
    }
    fn visible_checker_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "VisibleChecker",
            subs: calc_process_subs(),
            key: "visible_key",
            found: &VISIBLE_FOUND,
        })
    }

    #[test]
    fn chunk_scratch_visible_within_chunk_topo_order() {
        // GENERATOR priority fires before EMISSION_CALCULATOR. The calculator
        // must find the key the generator wrote in the same chunk context.
        VISIBLE_FOUND.store(false, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut registry = single_scratch_registry("VisibleWriter", "VisibleChecker");
        registry
            .register_generator("VisibleWriter", visible_writer_factory)
            .unwrap();
        registry
            .register_calculator("VisibleChecker", visible_checker_factory)
            .unwrap();
        let mut engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        engine.run().unwrap();
        assert!(
            VISIBLE_FOUND.load(Ordering::SeqCst),
            "calculator must find the key written by its preceding generator"
        );
    }

    // -- generator_write_then_calculator_read_same_chunk_roundtrips ---------

    static ROUNDTRIP_FOUND: AtomicBool = AtomicBool::new(false);

    fn roundtrip_writer_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "RoundtripWriter",
            subs: gen_process_subs(),
            key: "roundtrip_key",
        })
    }
    fn roundtrip_checker_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "RoundtripChecker",
            subs: calc_process_subs(),
            key: "roundtrip_key",
            found: &ROUNDTRIP_FOUND,
        })
    }

    #[test]
    fn generator_write_then_calculator_read_same_chunk_roundtrips() {
        // End-to-end roundtrip: generator inserts a DataFrame, calculator reads it.
        ROUNDTRIP_FOUND.store(false, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut registry = single_scratch_registry("RoundtripWriter", "RoundtripChecker");
        registry
            .register_generator("RoundtripWriter", roundtrip_writer_factory)
            .unwrap();
        registry
            .register_calculator("RoundtripChecker", roundtrip_checker_factory)
            .unwrap();
        let mut engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        engine.run().unwrap();
        assert!(
            ROUNDTRIP_FOUND.load(Ordering::SeqCst),
            "calculator must read back the DataFrame the generator wrote"
        );
    }

    // -- chunk_scratch_is_isolated_across_chunks ----------------------------

    static ISO_A_SAW_B: AtomicBool = AtomicBool::new(false);
    static ISO_B_SAW_A: AtomicBool = AtomicBool::new(false);

    fn iso_writer_a_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "IsoWriterA",
            subs: gen_process_subs(),
            key: "from_a",
        })
    }
    fn iso_writer_b_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "IsoWriterB",
            subs: gen_process_subs(),
            key: "from_b",
        })
    }
    fn iso_checker_a_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "IsoCheckerA",
            subs: calc_process_subs(),
            key: "from_b", // looks for chunk B's key — must NOT find it
            found: &ISO_A_SAW_B,
        })
    }
    fn iso_checker_b_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "IsoCheckerB",
            subs: calc_process_subs(),
            key: "from_a", // looks for chunk A's key — must NOT find it
            found: &ISO_B_SAW_A,
        })
    }

    fn iso_registry() -> CalculatorRegistry {
        let mut reg = two_chain_registry("IsoWriterA", "IsoCheckerA", "IsoWriterB", "IsoCheckerB");
        reg.register_generator("IsoWriterA", iso_writer_a_factory)
            .unwrap();
        reg.register_generator("IsoWriterB", iso_writer_b_factory)
            .unwrap();
        reg.register_calculator("IsoCheckerA", iso_checker_a_factory)
            .unwrap();
        reg.register_calculator("IsoCheckerB", iso_checker_b_factory)
            .unwrap();
        reg
    }

    #[test]
    fn chunk_scratch_is_isolated_across_chunks() {
        // Two independent chains run sequentially. Each checker looks for the
        // other chain's scratch key — isolated contexts must prevent leakage.
        ISO_A_SAW_B.store(false, Ordering::SeqCst);
        ISO_B_SAW_A.store(false, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.max_parallel_chunks = 1;
        let mut engine = MOVESEngine::new(sample_runspec(), iso_registry(), cfg);
        engine.run().unwrap();
        assert!(
            !ISO_A_SAW_B.load(Ordering::SeqCst),
            "chunk A's checker must not observe chunk B's scratch key"
        );
        assert!(
            !ISO_B_SAW_A.load(Ordering::SeqCst),
            "chunk B's checker must not observe chunk A's scratch key"
        );
    }

    // -- concurrent_chunks_do_not_observe_each_others_scratch ---------------

    static CONC_A_SAW_B: AtomicBool = AtomicBool::new(false);
    static CONC_B_SAW_A: AtomicBool = AtomicBool::new(false);

    fn conc_writer_a_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "ConcWriterA",
            subs: gen_process_subs(),
            key: "conc_from_a",
        })
    }
    fn conc_writer_b_factory() -> Box<dyn Generator> {
        Box::new(ScratchWriterGen {
            name: "ConcWriterB",
            subs: gen_process_subs(),
            key: "conc_from_b",
        })
    }
    fn conc_checker_a_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "ConcCheckerA",
            subs: calc_process_subs(),
            key: "conc_from_b",
            found: &CONC_A_SAW_B,
        })
    }
    fn conc_checker_b_factory() -> Box<dyn Calculator> {
        Box::new(ScratchCheckCalc {
            name: "ConcCheckerB",
            subs: calc_process_subs(),
            key: "conc_from_a",
            found: &CONC_B_SAW_A,
        })
    }

    #[test]
    fn concurrent_chunks_do_not_observe_each_others_scratch() {
        // Two independent chains, parallel execution. Each chunk's
        // CalculatorContext is a separate Arc<Mutex<…>> so concurrent
        // execution cannot cause cross-chunk scratch leakage.
        CONC_A_SAW_B.store(false, Ordering::SeqCst);
        CONC_B_SAW_A.store(false, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.max_parallel_chunks = 2;
        let mut reg =
            two_chain_registry("ConcWriterA", "ConcCheckerA", "ConcWriterB", "ConcCheckerB");
        reg.register_generator("ConcWriterA", conc_writer_a_factory)
            .unwrap();
        reg.register_generator("ConcWriterB", conc_writer_b_factory)
            .unwrap();
        reg.register_calculator("ConcCheckerA", conc_checker_a_factory)
            .unwrap();
        reg.register_calculator("ConcCheckerB", conc_checker_b_factory)
            .unwrap();
        let mut engine = MOVESEngine::new(sample_runspec(), reg, cfg);
        engine.run().unwrap();
        assert!(
            !CONC_A_SAW_B.load(Ordering::SeqCst),
            "concurrent chunk A must not observe chunk B's scratch"
        );
        assert!(
            !CONC_B_SAW_A.load(Ordering::SeqCst),
            "concurrent chunk B must not observe chunk A's scratch"
        );
    }

    // ---- Streaming aggregation regression ----------------------------------
    //
    // Verifies that a calculator producing emission DataFrames has its rows
    // folded into the streaming aggregator and written to Parquet correctly.
    // Raw row accumulation is replaced by streaming aggregation, so peak
    // in-memory residency is bounded by N_distinct_groups × record_size.

    static EMITTING_RUNS: AtomicUsize = AtomicUsize::new(0);

    /// Calculator that returns a tiny emission DataFrame (two rows, same
    /// pollutant/year/month/day/hour) on every invocation. Used to verify
    /// the streaming aggregation path end-to-end.
    #[derive(Debug)]
    struct EmittingCalc {
        subs: Vec<CalculatorSubscription>,
    }

    impl Calculator for EmittingCalc {
        fn name(&self) -> &'static str {
            "EmittingCalc"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            &self.subs
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &[]
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput> {
            EMITTING_RUNS.fetch_add(1, Ordering::SeqCst);
            use polars::prelude::{DataFrame, NamedFrom, Series};
            // Emit two rows for pollutant 2 (CO), emissionQuant 1.0 and 2.0.
            // Both rows share identical key columns so they roll into one
            // aggregated row with emissionQuant = 3.0.
            let df = DataFrame::new(
                2,
                vec![
                    Series::new("pollutantID".into(), [2i32, 2i32]).into(),
                    Series::new("yearID".into(), [2020i32, 2020i32]).into(),
                    Series::new("monthID".into(), [7i32, 7i32]).into(),
                    Series::new("dayID".into(), [5i32, 5i32]).into(),
                    Series::new("hourID".into(), [8i32, 8i32]).into(),
                    Series::new("processID".into(), [1i32, 1i32]).into(),
                    Series::new("emissionQuant".into(), [1.0f64, 2.0f64]).into(),
                ],
            )
            .unwrap();
            Ok(CalculatorOutput::with_dataframe(df))
        }
    }

    fn emitting_registry() -> CalculatorRegistry {
        let info = parse_calculator_info_str(
            "Registration\tCO\t2\tRunning Exhaust\t1\tEmittingCalc\n\
             Subscribe\tEmittingCalc\tRunning Exhaust\t1\tPROCESS\tEMISSION_CALCULATOR\n",
            Path::new("test"),
        )
        .unwrap();
        CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
    }

    fn emitting_calc_factory() -> Box<dyn Calculator> {
        Box::new(EmittingCalc {
            subs: process_subs(),
        })
    }

    #[test]
    fn streaming_agg_folds_emission_rows_and_writes_parquet() {
        EMITTING_RUNS.store(0, Ordering::SeqCst);
        let dir = tempdir().unwrap();
        let mut registry = emitting_registry();
        registry
            .register_calculator("EmittingCalc", emitting_calc_factory)
            .unwrap();
        let mut engine = MOVESEngine::new(sample_runspec(), registry, config(dir.path()));
        engine.run().unwrap();

        // The calculator fired once (one process × one PROCESS-granularity sub).
        assert_eq!(EMITTING_RUNS.load(Ordering::SeqCst), 1);

        // The emission Parquet must exist under MOVESOutput/.
        // sample_runspec is Hour timestep (years=[2020], months=[7], days=[5],
        // hours=[8]), so the partition is yearID=2020/monthID=7.
        let partition = dir
            .path()
            .join("MOVESOutput/yearID=2020/monthID=7/part.parquet");
        assert!(partition.exists(), "emission partition must be written");

        // Read it back and check row count and summed emissionQuant.
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let raw = std::fs::read(&partition).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        // Hour + County aggregation: all dimensions kept, so each raw row
        // may map to a distinct group. Two rows with the same keys (same
        // pollutant/year/month/day/hour) should aggregate into one row with
        // emissionQuant = 1.0 + 2.0 = 3.0.
        let quant_idx = batch.schema().index_of("emissionQuant").unwrap();
        let quant_col = batch
            .column(quant_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let total: f64 = (0..quant_col.len()).map(|i| quant_col.value(i)).sum();
        assert!(
            (total - 3.0).abs() < 1e-9,
            "expected emissionQuant sum 3.0, got {total}"
        );
    }
}
