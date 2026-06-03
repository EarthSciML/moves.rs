//! Calculator and Generator base traits — the contract
//! calculators and generators implement.
//!
//! Ports `EmissionCalculator.java`, `Generator.java`, and the abstract base
//! `GenericCalculatorBase.java` from `gov.epa.otaq.moves.master.framework`.
//!
//! # The two traits
//!
//! [`Calculator`] and [`Generator`] are parallel traits with overlapping
//! shape:
//!
//! * Both expose **metadata methods** — [`name`](Calculator::name),
//! [`subscriptions`](Calculator::subscriptions),
//! [`upstream`](Calculator::upstream),
//! [`input_tables`](Calculator::input_tables).'s
//! `CalculatorRegistry` reads these to wire the MasterLoop without
//! running any calculator body.
//! * Both expose a **work method**//! `execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput>`.
//! loop notifier invokes this once per iteration at the
//! registered granularity.
//!
//! Calculator-specific:
//! * [`Calculator::registrations`] — the `(pollutant, process)` pairs the
//! calculator advertises in `Registration` directives.
//!
//! Generator-specific:
//! * [`Generator::output_tables`] — scratch-namespace tables this generator
//! writes for downstream calculators.
//!
//! # Relationship to [`MasterLoopable`](crate::MasterLoopable)
//!
//! Neither [`Calculator`] nor [`Generator`] inherit
//! [`MasterLoopable`](crate::MasterLoopable). Doing so would force a
//! default `execute_at_granularity` body, but the
//! [`MasterLoopContext`](crate::MasterLoopContext) the trait method receives
//! does not yet hold a [`CalculatorContext`] — that pairing is owned by
//! registry, which holds the per-run state needed to materialise
//! the calculator context from a master-loop callback.
//!
//! will instead provide an adapter (`CalculatorMasterLoopable`
//! or a generic equivalent) that wraps a [`Calculator`]/[`Generator`] plus a
//! handle to the run state, implements [`MasterLoopable`](crate::MasterLoopable)
//! for that wrapper, and translates each loop callback into a
//! [`Calculator::execute`] / [`Generator::execute`] invocation against a
//! freshly built [`CalculatorContext`].
//!
//! Keeping `Calculator` decoupled from `MasterLoopable` means
//! calculator authors only need to think about "given this context, produce
//! this output" — the dispatch plumbing stays inside the framework.
//!
//! # Context shape
//!
//! [`CalculatorContext`] is the runtime view of a calculator's inputs//! per-run filtered default-DB tables ([`crate::data::InMemoryStore`]), inter-calculator
//! scratch ([`ScratchNamespace`]), and the master loop's current
//! [`IterationPosition`] (process, location, time). The component types
//! live in [`crate::execution::execution_db`]; this module re-binds them onto the
//! trait signatures.
//!
//! [`CalculatorOutput`] is the per-invocation result type.
//! skeleton.** (`DataFrameStore`) replaces it with a Polars
//! `DataFrame` once the data plane lands. Sealing the trait signature
//! around the named placeholder type lets calculators start
//! porting against a stable API today; widening the placeholder later
//! does not break implementors.

use std::sync::Arc;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_runspec::model::{ModelDomain, ModelScale};
use polars::prelude::DataFrame;

use crate::data::InMemoryStore;
use crate::error::Error;
use crate::execution::execution_db::{IterationPosition, ScratchNamespace};

/// Runtime view of a calculator's inputs and scratch space — the in-memory
/// equivalent of MOVES's MariaDB execution database.
///
/// Owns the three pieces specifies:
///
/// * [`tables`](Self::tables) — per-run filtered default-DB tables,
/// loaded once per run (slow tier).
/// * [`scratch`](Self::scratch) — name-keyed inter-calculator scratch
/// space (fast tier).
/// * [`position`](Self::position) — the current MasterLoop iteration /
/// location / time triple.
///
/// Calculators don't construct [`CalculatorContext`] themselves;'s
/// registry materialises a fresh one on each
/// [`MasterLoopable::execute_at_granularity`](crate::MasterLoopable::execute_at_granularity)
/// callback and passes it by reference to [`Calculator::execute`] /
/// [`Generator::execute`].
///
/// The slow and scratch tiers are backed by [`InMemoryStore`].
/// authors read `ctx.tables()`, write scratch via `ctx.scratch_mut()`, and
/// read `ctx.position().location.county_id`, `ctx.position().time.hour`, etc.
#[derive(Debug, Default)]
pub struct CalculatorContext {
    /// Shared, read-only slow-tier tables loaded once per run by
    /// `InputDataManager`. The `Arc` allows all chunks in a run to share
    /// the same loaded store without copying.
    slow: Arc<InMemoryStore>,
    /// Per-chunk scratch namespace. Generators write here; downstream
    /// calculators in the same chunk read. Each chunk owns an independent
    /// `ScratchNamespace` so there is no cross-chunk scratch leakage.
    scratch: ScratchNamespace,
    position: IterationPosition,
    /// The run's [`ModelScale`] (`targetRunSpec.scale`). `None` in the
    /// default/test contexts that don't model a full run; the engine sets it
    /// per chunk so scale-sensitive calculators (e.g. `BaseRateCalculator`'s
    /// inventory activity weighting) can branch on it without re-deriving the
    /// runspec. Mirrors `ExecutionRunSpec.getModelScale()` used by the Java
    /// `BaseRateCalculator.doExecute`.
    scale: Option<ModelScale>,
    /// The run's [`ModelDomain`] (`targetRunSpec.domain`). `None` in default/test
    /// contexts. The engine sets it so domain-sensitive calculators (e.g.
    /// `BaseRateGenerator`) can distinguish Project from non-Project domain without
    /// re-reading the RunSpec. Mirrors `ExecutionRunSpec.getModelDomain()` /
    /// `configuration.Singleton.IsProject` in the Go worker.
    domain: Option<ModelDomain>,
    /// Per-run behavioral tokens from the worker's `-parameters=` CSV
    /// (everything except the trailing processID, yearID, roadTypeID integers).
    /// The engine derives these from the RunSpec and stores them here so
    /// calculators can pass them — augmented with per-iteration integers — to
    /// [`moves_calculators`] parsers like `ExternalFlags::from_parameters`.
    /// Empty in default/test contexts.
    parameters: Vec<String>,
}

impl CalculatorContext {
    /// Construct an empty context with start-of-run position.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a context with the given position, leaving tables and
    /// scratch empty. Convenient for tests that exercise position-aware
    /// logic without touching the data plane.
    #[must_use]
    pub fn with_position(position: IterationPosition) -> Self {
        Self {
            slow: Arc::default(),
            scratch: ScratchNamespace::empty(),
            position,
            scale: None,
            domain: None,
            parameters: Vec::new(),
        }
    }

    /// Construct a context with pre-populated slow-tier store and
    /// start-of-run position. Convenient for tests that seed the slow
    /// tier before exercising a calculator body.
    #[must_use]
    pub fn with_tables(store: InMemoryStore) -> Self {
        Self {
            slow: Arc::new(store),
            scratch: ScratchNamespace::empty(),
            position: IterationPosition::default(),
            scale: None,
            domain: None,
            parameters: Vec::new(),
        }
    }

    /// Construct a context with the given position and pre-populated
    /// slow-tier store.
    #[must_use]
    pub fn with_position_and_tables(position: IterationPosition, store: InMemoryStore) -> Self {
        Self {
            slow: Arc::new(store),
            scratch: ScratchNamespace::empty(),
            position,
            scale: None,
            domain: None,
            parameters: Vec::new(),
        }
    }

    /// Construct a context sharing an existing slow-tier `Arc<InMemoryStore>`.
    /// Used by the engine to give each chunk access to the run-level slow
    /// store without cloning the data.
    #[must_use]
    pub fn with_slow(slow: Arc<InMemoryStore>) -> Self {
        Self {
            slow,
            scratch: ScratchNamespace::empty(),
            position: IterationPosition::default(),
            scale: None,
            domain: None,
            parameters: Vec::new(),
        }
    }

    /// Per-run filtered default-DB tables. Calculators read from this in
    /// their [`Calculator::execute`] body, indexing by the canonical
    /// table names declared in [`Calculator::input_tables`].
    #[must_use]
    pub fn tables(&self) -> &InMemoryStore {
        &self.slow
    }

    /// Mutable access to the slow-tier store. Uses `Arc::make_mut` so the
    /// store is cloned only when the `Arc` has multiple owners.
    pub fn tables_mut(&mut self) -> &mut InMemoryStore {
        Arc::make_mut(&mut self.slow)
    }

    /// Inter-calculator scratch namespace. Calculators read from here.
    #[must_use]
    pub fn scratch(&self) -> &ScratchNamespace {
        &self.scratch
    }

    /// Mutable scratch namespace. Generators write here in their
    /// [`Generator::execute`] body; downstream calculators in the same
    /// chunk read via [`scratch`](Self::scratch).
    pub fn scratch_mut(&mut self) -> &mut ScratchNamespace {
        &mut self.scratch
    }

    /// Make generator scratch output visible to calculators that read the slow
    /// tier through [`tables`](Self::tables).
    ///
    /// Generators write their output tables (SHO, BaseRateByAge, …) to
    /// [`scratch_mut`](Self::scratch_mut), but most calculators read every
    /// input — default-DB and generator-produced alike — through
    /// `ctx.tables()`. This copies the scratch tables into the slow store
    /// (cheap `Arc` clones; `Arc::make_mut` clones the chunk's slow store only
    /// on first write) so both read paths resolve generator output. Scratch is
    /// left intact, so the calculators that read `ctx.scratch()` directly keep
    /// working.
    pub fn promote_scratch(&mut self) {
        if self.scratch.store.is_empty() {
            return;
        }
        let slow = std::sync::Arc::make_mut(&mut self.slow);
        self.scratch.store.copy_into(slow);
    }

    /// Current MasterLoop iteration / location / time triple.
    #[must_use]
    pub fn position(&self) -> &IterationPosition {
        &self.position
    }

    /// Update the iteration position. Called by the engine adapter before
    /// each module invocation so the context reflects the loop's current
    /// position without replacing the shared slow tier or scratch.
    pub fn set_position(&mut self, position: IterationPosition) {
        self.position = position;
    }

    /// The run's [`ModelScale`], if the engine set it. `None` in default/test
    /// contexts.
    #[must_use]
    pub fn model_scale(&self) -> Option<ModelScale> {
        self.scale
    }

    /// Set the run's [`ModelScale`]. The engine calls this once per chunk
    /// context so scale-sensitive calculator bodies can read it.
    pub fn set_model_scale(&mut self, scale: ModelScale) {
        self.scale = Some(scale);
    }

    /// The run's [`ModelDomain`], if the engine set it. `None` in default/test
    /// contexts. Mirrors `ExecutionRunSpec.getModelDomain()` / the Go worker's
    /// `configuration.Singleton.IsProject`.
    #[must_use]
    pub fn model_domain(&self) -> Option<ModelDomain> {
        self.domain
    }

    /// Set the run's [`ModelDomain`]. The engine calls this once per chunk
    /// context so domain-sensitive calculators (e.g. `BaseRateGenerator`) can
    /// branch on Project vs non-Project domain without re-reading the RunSpec.
    pub fn set_model_domain(&mut self, domain: Option<ModelDomain>) {
        self.domain = domain;
    }

    /// Whether the run is in Project domain — equivalent to the Go worker's
    /// `configuration.Singleton.IsProject`.
    #[must_use]
    pub fn is_project(&self) -> bool {
        self.domain == Some(ModelDomain::Project)
    }

    /// Whether the run is in Single (County) domain — county-scale run with a
    /// County Data Manager (CDB) input. County-scale CDB tables are loaded into
    /// the slow store and override the default-DB tables for the tables they cover.
    #[must_use]
    pub fn is_single(&self) -> bool {
        self.domain == Some(ModelDomain::Single)
    }

    /// Whether the run uses a user-supplied scale input database — true for both
    /// Single (County) and Project domain. CDB/PDB Parquet tables override the
    /// default-DB tables for the tables they cover.
    #[must_use]
    pub fn has_scale_input(&self) -> bool {
        matches!(
            self.domain,
            Some(ModelDomain::Single) | Some(ModelDomain::Project)
        )
    }

    /// Per-run behavioral tokens from the worker's `-parameters=` CSV (not
    /// including the trailing processID, yearID, roadTypeID integers). The
    /// engine populates this from the RunSpec so calculators that call
    /// `ExternalFlags::from_parameters` can prepend these run-level tokens to
    /// their per-iteration integers. Empty in default/test contexts.
    #[must_use]
    pub fn parameters(&self) -> &[String] {
        &self.parameters
    }

    /// Set the per-run behavioral parameter tokens. Called by the engine once
    /// per chunk context from the RunSpec.
    pub fn set_parameters(&mut self, parameters: Vec<String>) {
        self.parameters = parameters;
    }
}

/// Value returned by [`Calculator::execute`] / [`Generator::execute`].
///
/// Wraps an optional Polars [`DataFrame`]: calculators that produce activity
/// output fill it with [`CalculatorOutput::with_dataframe`]; generators or
/// calculators that write only to scratch (or produce no direct output)
/// return [`CalculatorOutput::empty`].
#[derive(Debug, Default)]
pub struct CalculatorOutput {
    dataframe: Option<DataFrame>,
}

impl CalculatorOutput {
    /// Construct an output carrying no DataFrame — for calculators that write
    /// only to scratch or produce no direct output.
    #[must_use]
    pub fn empty() -> Self {
        Self { dataframe: None }
    }

    /// Construct an output wrapping `df` — for calculators that return a
    /// result table (e.g. `MOVESWorkerActivityOutput`).
    #[must_use]
    pub fn with_dataframe(df: DataFrame) -> Self {
        Self {
            dataframe: Some(df),
        }
    }

    /// Borrow the contained DataFrame, if any.
    #[must_use]
    pub fn dataframe(&self) -> Option<&DataFrame> {
        self.dataframe.as_ref()
    }

    /// Consume `self` and return the owned DataFrame, if any.
    #[must_use]
    pub fn into_dataframe(self) -> Option<DataFrame> {
        self.dataframe
    }
}

/// One MasterLoop subscription declared by a calculator or generator/// the Rust analogue of one `Subscribe` directive in `CalculatorInfo.txt`.
///
/// A single module may carry multiple subscriptions: e.g. a calculator that
/// covers both running and start exhaust processes at the same granularity
/// would declare two `CalculatorSubscription` rows.
///
/// Priority is stored as the rich [`Priority`] type so the source reads as
/// `Priority::parse("EMISSION_CALCULATOR+1").unwrap()` rather than as a
/// magic-number `11`. registry calls [`Priority::value`] when
/// composing the underlying [`MasterLoopableSubscription`](crate::MasterLoopableSubscription).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalculatorSubscription {
    /// MOVES process this subscription is gated to.
    pub process_id: ProcessId,
    /// Granularity bucket the subscription fires in.
    pub granularity: Granularity,
    /// Priority within the granularity bucket.
    pub priority: Priority,
}

impl CalculatorSubscription {
    /// Construct a subscription from its three components.
    #[must_use]
    pub fn new(process_id: ProcessId, granularity: Granularity, priority: Priority) -> Self {
        Self {
            process_id,
            granularity,
            priority,
        }
    }
}

/// Contract every MOVES emission calculator implements.
///
/// Ports `EmissionCalculator.java` plus the metadata stubs in
/// `GenericCalculatorBase.java`. lands one
/// implementor per Java calculator class.
///
/// A `Calculator` is a value type that owns no per-run state —'s
/// registry instantiates one per chain-DAG entry and reuses it across the
/// run. All run-varying inputs flow through the [`CalculatorContext`]
/// argument to [`execute`](Self::execute).
pub trait Calculator: Send + Sync + std::fmt::Debug {
    /// Stable identifier matching the calculator's name in the chain DAG
    /// (the `ModuleEntry::name` from
    /// [`moves_calculator_info::CalculatorDag::modules`]).
    /// Used by the registry to wire chain edges between calculators.
    fn name(&self) -> &'static str;

    /// `(process, granularity, priority)` triples this calculator subscribes
    /// to. Most calculators return a single-entry slice; a few subscribe
    /// at multiple granularities for different processes — those return one
    /// row per `Subscribe` directive recorded in `CalculatorInfo.txt`.
    fn subscriptions(&self) -> &[CalculatorSubscription];

    /// `(pollutant, process)` pairs this calculator produces output for /// the `Registration` directives in `CalculatorInfo.txt`.
    ///
    /// Returned slice is allowed to be empty for the rare calculator that
    /// only produces intermediate scratch data and registers nothing for
    /// direct output. Most calculators register at least one pair.
    fn registrations(&self) -> &[PollutantProcessAssociation];

    /// Names of upstream calculators/generators whose output this
    /// calculator's chain consumes. Each name matches a `ModuleEntry::name`
    /// elsewhere in the registry — the registry validates closure during
    /// startup.
    ///
    /// Default empty: root subscribers don't depend on anything upstream.
    fn upstream(&self) -> &[&'static str] {
        &[]
    }

    /// Default-DB / per-run scratch tables this calculator reads from
    /// [`CalculatorContext`]. (`InputDataManager`) uses these to
    /// drive lazy loading and dependency analysis.
    ///
    /// Names are the canonical default-DB table names (e.g.
    /// `"sourceUseTypePopulation"`); the registry maps them onto Parquet
    /// snapshot files when materialising the context.
    ///
    /// Default empty: calculator authors fill this in as they port.
    fn input_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Pollutants this (chained) calculator **consumes and replaces** in the
    /// worker output — the canonical `delete from MOVESWorkerOutput where
    /// pollutantID = …` that a speciation calculator runs before re-inserting
    /// its adjusted/split values (e.g. `SulfatePMCalculator` deletes EC 112 and
    /// NonECPM 118).
    ///
    /// The additive chained engine cannot delete a row, so an upstream
    /// producer's *zero-valued* row for such a pollutant — which the chained
    /// calculator's per-key delta cannot cancel (`0 − 0 = 0`) — would survive
    /// into the final output where canonical removed it (e.g. BaseRate's
    /// fuelType-9 electricity EC/NonECPM zeros). The engine therefore drops a
    /// producer's zero-valued row for any replaced pollutant before it reaches
    /// the output aggregator; it still reaches the per-chunk worker accumulator
    /// the chained calculator reads, and non-zero rows are kept and corrected by
    /// the chained calculator's delta. Canonical never emits a zero row for a
    /// replaced pollutant, so this is exact.
    ///
    /// Default empty: only consume/replace speciation calculators override it.
    fn replaced_pollutants(&self) -> &[i32] {
        &[]
    }

    /// Run the calculator. Called once per iteration at the registered
    /// granularity. Returns a [`CalculatorOutput`] (: a Polars
    /// `DataFrame`) of emission rows ready to merge into the master output.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error>;
}

/// Contract every MOVES generator implements.
///
/// Ports `Generator.java`. Generators produce upstream activity, fuel-effect,
/// or operating-mode data that emission calculators consume from the scratch
/// namespace. They subscribe to the MasterLoop like calculators but do not
/// register `(pollutant, process)` pairs — their output is intermediate,
/// not part of the emission tally.
///
/// generator implementations live in `moves-calculators` alongside
/// the calculators (the chain DAG groups them together).
pub trait Generator: Send + Sync + std::fmt::Debug {
    /// Stable identifier matching the generator's name in the chain DAG.
    fn name(&self) -> &'static str;

    /// `(process, granularity, priority)` triples this generator subscribes
    /// to. Generators typically subscribe at a coarser granularity than the
    /// calculators that consume their output (e.g. PROCESS or COUNTY), so
    /// the generated tables are reused across many inner-loop iterations.
    fn subscriptions(&self) -> &[CalculatorSubscription];

    /// Names of upstream generators/calculators this generator depends on.
    /// Defaults to empty.
    fn upstream(&self) -> &[&'static str] {
        &[]
    }

    /// Default-DB / per-run scratch tables this generator reads from
    /// [`CalculatorContext`]. Defaults to empty.
    fn input_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Scratch-namespace tables this generator writes. Downstream
    /// calculators name these in their own [`Calculator::input_tables`] to
    /// declare the dependency. Defaults to empty.
    fn output_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Run the generator. Called once per iteration at the registered
    /// granularity. The returned [`CalculatorOutput`] is stored under the
    /// generator's [`output_tables`](Self::output_tables) names by the
    /// registry.
    ///
    /// Receives `&mut CalculatorContext` so the generator can write to
    /// [`CalculatorContext::scratch_mut`]. The mutable borrow is
    /// exclusive within the chunk's sequential execution — no aliasing.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::PollutantId;

    /// Minimal `Calculator` implementor exercising every trait method /// shape check for the API. The body returns
    /// [`CalculatorOutput::empty`] so this is a compile-and-call smoke test,
    /// not a numerical check.
    #[derive(Debug)]
    struct DummyCalculator;

    static DUMMY_CALC_SUBS: &[CalculatorSubscription] = &[];
    static DUMMY_CALC_REGS: &[PollutantProcessAssociation] = &[];
    static DUMMY_CALC_UPSTREAM: &[&str] = &["UpstreamCalculator"];
    static DUMMY_CALC_INPUTS: &[&str] = &["sourceUseTypePopulation"];

    impl Calculator for DummyCalculator {
        fn name(&self) -> &'static str {
            "DummyCalculator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            DUMMY_CALC_SUBS
        }
        fn registrations(&self) -> &[PollutantProcessAssociation] {
            DUMMY_CALC_REGS
        }
        fn upstream(&self) -> &[&'static str] {
            DUMMY_CALC_UPSTREAM
        }
        fn input_tables(&self) -> &[&'static str] {
            DUMMY_CALC_INPUTS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    /// Minimal `Generator` implementor exercising every trait method.
    #[derive(Debug)]
    struct DummyGenerator;

    static DUMMY_GEN_SUBS: &[CalculatorSubscription] = &[];
    static DUMMY_GEN_OUTPUTS: &[&str] = &["sourceBinDistribution"];

    impl Generator for DummyGenerator {
        fn name(&self) -> &'static str {
            "DummyGenerator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            DUMMY_GEN_SUBS
        }
        fn output_tables(&self) -> &[&'static str] {
            DUMMY_GEN_OUTPUTS
        }
        fn execute(&self, _ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    #[test]
    fn calculator_metadata_and_execute_compile_and_call() {
        let calc = DummyCalculator;
        assert_eq!(calc.name(), "DummyCalculator");
        assert!(calc.subscriptions().is_empty());
        assert!(calc.registrations().is_empty());
        assert_eq!(calc.upstream(), &["UpstreamCalculator"]);
        assert_eq!(calc.input_tables(), &["sourceUseTypePopulation"]);
        let ctx = CalculatorContext::new();
        let _out: CalculatorOutput = calc.execute(&ctx).expect("execute ok");
    }

    #[test]
    fn generator_metadata_and_execute_compile_and_call() {
        let gen = DummyGenerator;
        assert_eq!(gen.name(), "DummyGenerator");
        assert!(gen.subscriptions().is_empty());
        // Default-impl defaults exercise — upstream + input_tables not overridden.
        assert!(gen.upstream().is_empty());
        assert!(gen.input_tables().is_empty());
        assert_eq!(gen.output_tables(), &["sourceBinDistribution"]);
        let mut ctx = CalculatorContext::new();
        let _out: CalculatorOutput = gen.execute(&mut ctx).expect("execute ok");
    }

    #[test]
    fn calculator_can_be_held_as_trait_object() {
        // The registry will store calculators as `Box<dyn Calculator>`;
        // verify the trait is object-safe and a calculator value can be coerced.
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(DummyCalculator)];
        assert_eq!(calcs[0].name(), "DummyCalculator");
    }

    #[test]
    fn generator_can_be_held_as_trait_object() {
        let gens: Vec<Box<dyn Generator>> = vec![Box::new(DummyGenerator)];
        assert_eq!(gens[0].name(), "DummyGenerator");
    }

    #[test]
    fn calculator_subscription_round_trip() {
        // Build a subscription with realistic process + granularity + priority
        // and assert the components stick.
        let sub = CalculatorSubscription::new(
            ProcessId(1), // Running Exhaust
            Granularity::Hour,
            Priority::parse("EMISSION_CALCULATOR+1").unwrap(),
        );
        assert_eq!(sub.process_id, ProcessId(1));
        assert_eq!(sub.granularity, Granularity::Hour);
        assert_eq!(sub.priority.display(), "EMISSION_CALCULATOR+1");
    }

    #[test]
    fn calculator_subscription_is_copy_and_eq() {
        // Subscriptions need `Copy` for the slice-returning trait method API:
        // calculator structs hold them in `static` arrays without `Clone` boilerplate.
        let a = CalculatorSubscription::new(
            ProcessId(2),
            Granularity::Hour,
            Priority::parse("EMISSION_CALCULATOR").unwrap(),
        );
        let b = a;
        assert_eq!(a, b);
    }

    /// Realistic shape test: a calculator with non-empty registrations
    /// and a multi-process subscription set, mirroring how
    /// calculators will look once they land. Uses canonical `Pollutant` /
    /// `Process` ids from the MOVES default DB.
    #[derive(Debug)]
    struct ExampleRealisticCalculator;

    static EX_SUBS: &[CalculatorSubscription] = &[];
    static EX_REGS: &[PollutantProcessAssociation] = &[
        PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: ProcessId(1),
        }, // CO2 Running Exhaust
        PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: ProcessId(2),
        }, // CO2 Start Exhaust
    ];
    static EX_UPSTREAM: &[&str] = &["TotalActivityGenerator"];
    static EX_INPUTS: &[&str] = &["sourceUseTypePopulation", "emissionRateByAge"];

    impl Calculator for ExampleRealisticCalculator {
        fn name(&self) -> &'static str {
            "ExampleRealisticCalculator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            EX_SUBS
        }
        fn registrations(&self) -> &[PollutantProcessAssociation] {
            EX_REGS
        }
        fn upstream(&self) -> &[&'static str] {
            EX_UPSTREAM
        }
        fn input_tables(&self) -> &[&'static str] {
            EX_INPUTS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    #[test]
    fn realistic_calculator_shape() {
        let calc = ExampleRealisticCalculator;
        let regs = calc.registrations();
        assert_eq!(regs.len(), 2);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(2)));
        let procs: Vec<ProcessId> = regs.iter().map(|r| r.process_id).collect();
        assert_eq!(procs, vec![ProcessId(1), ProcessId(2)]);
        assert_eq!(calc.upstream().len(), 1);
        assert_eq!(calc.input_tables().len(), 2);
    }

    #[test]
    fn context_default_position_is_start_of_run() {
        // The default-constructed context puts the master loop at the
        // pre-iteration state — no process, no location, no time.
        let ctx = CalculatorContext::new();
        let pos = ctx.position();
        assert_eq!(pos.iteration, 0);
        assert!(pos.process_id.is_none());
        assert!(pos.location.county_id.is_none());
        assert!(pos.time.year.is_none());
    }

    #[test]
    fn context_with_position_carries_through_execute() {
        // Calculators reach the position through `ctx.position()`. Build a
        // context at HOUR granularity and verify a Calculator body can
        // read each component from the accessor chain.
        use crate::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::link(40, 40_001, 400_011, 4_000_111),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        let ctx = CalculatorContext::with_position(pos);
        let calc = DummyCalculator;
        // Calculator body doesn't read the context — just verify it can
        // be called with the populated context.
        let _out = calc.execute(&ctx).expect("execute ok");
        // And verify the position is readable through the public accessor
        // (matches what calculator bodies will do).
        assert_eq!(ctx.position().process_id, Some(ProcessId(1)));
        assert_eq!(ctx.position().location.state_id, Some(40));
        assert_eq!(ctx.position().location.link_id, Some(4_000_111));
        assert_eq!(ctx.position().time.hour, Some(8));
    }

    #[test]
    fn context_domain_and_is_project_accessors() {
        use moves_runspec::model::ModelDomain;
        let mut ctx = CalculatorContext::new();
        assert_eq!(ctx.model_domain(), None, "default domain is None");
        assert!(!ctx.is_project(), "None domain → is_project() is false");

        ctx.set_model_domain(Some(ModelDomain::Project));
        assert_eq!(ctx.model_domain(), Some(ModelDomain::Project));
        assert!(ctx.is_project(), "Project domain → is_project() is true");

        ctx.set_model_domain(Some(ModelDomain::Default));
        assert!(!ctx.is_project(), "Default domain is not project");

        ctx.set_model_domain(None);
        assert!(!ctx.is_project(), "None domain is not project");
    }

    #[test]
    fn context_is_single_and_has_scale_input() {
        use moves_runspec::model::ModelDomain;
        let mut ctx = CalculatorContext::new();

        // None domain: neither Single nor has_scale_input.
        assert!(!ctx.is_single(), "None → not single");
        assert!(!ctx.has_scale_input(), "None → no scale input");

        // Default domain: neither.
        ctx.set_model_domain(Some(ModelDomain::Default));
        assert!(!ctx.is_single(), "Default → not single");
        assert!(!ctx.has_scale_input(), "Default → no scale input");

        // Single domain: is_single=true, has_scale_input=true.
        ctx.set_model_domain(Some(ModelDomain::Single));
        assert!(ctx.is_single(), "Single → is_single()");
        assert!(ctx.has_scale_input(), "Single → has_scale_input()");
        assert!(!ctx.is_project(), "Single → not project");

        // Project domain: is_single=false, has_scale_input=true.
        ctx.set_model_domain(Some(ModelDomain::Project));
        assert!(!ctx.is_single(), "Project → not single");
        assert!(ctx.has_scale_input(), "Project → has_scale_input()");
        assert!(ctx.is_project(), "Project → is_project()");
    }

    #[test]
    fn context_parameters_default_empty_and_round_trip() {
        let mut ctx = CalculatorContext::new();
        assert!(
            ctx.parameters().is_empty(),
            "default parameters must be empty"
        );
        let tokens = vec!["yOp".to_string(), "nASB".to_string(), "1".to_string()];
        ctx.set_parameters(tokens.clone());
        assert_eq!(ctx.parameters(), tokens.as_slice());
        // Overwrite clears the previous value.
        ctx.set_parameters(vec![]);
        assert!(ctx.parameters().is_empty());
    }
}
